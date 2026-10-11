package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/ddejongh/venuepulse/backend-go/internal/domain"
	"github.com/ddejongh/venuepulse/backend-go/internal/store/sqlc"
)

// SampleRow is one metric_samples row. (SourceID, EventID) is its
// idempotency key.
type SampleRow struct {
	SourceID string
	EventID  string
	TS       time.Time
	VenueID  string
	ZoneID   string
	Metric   string
	Value    float64
}

// IncidentRow is an incident reported by an ingest event.
type IncidentRow struct {
	SourceID    string
	EventID     string
	VenueID     string
	ZoneID      string
	Severity    string
	Category    string
	Description string
	ReportedAt  time.Time
}

type IngestBatch struct {
	Samples   []SampleRow
	Incidents []IncidentRow
}

type IngestResult struct {
	// SamplesInserted excludes duplicates of already-ingested events.
	SamplesInserted int64
	// NewIncidents holds only incidents created by this call; redelivered
	// events produce none.
	NewIncidents []domain.Incident
}

// Ingest writes a batch in one transaction, creating daily partitions as
// needed. It is idempotent: replaying a batch inserts nothing new.
func (s *Store) Ingest(ctx context.Context, b IngestBatch) (IngestResult, error) {
	var res IngestResult
	newDays := s.uncachedDays(b.Samples)

	err := s.inTx(ctx, func(q *sqlc.Queries) error {
		res = IngestResult{} // reset if fn is ever retried
		for _, day := range newDays {
			if err := q.EnsureMetricSamplesPartition(ctx, pgtype.Date{Time: day, Valid: true}); err != nil {
				return fmt.Errorf("ensure partition %s: %w", day.Format(time.DateOnly), err)
			}
		}

		if len(b.Samples) > 0 {
			n, err := q.InsertMetricSamples(ctx, sampleColumns(b.Samples))
			if err != nil {
				return fmt.Errorf("insert %d metric samples: %w", len(b.Samples), err)
			}
			res.SamplesInserted = n
		}

		for _, in := range b.Incidents {
			row, err := q.InsertIncidentFromEvent(ctx, sqlc.InsertIncidentFromEventParams{
				VenueID:     in.VenueID,
				ZoneID:      in.ZoneID,
				Severity:    in.Severity,
				Category:    in.Category,
				Description: in.Description,
				ReportedAt:  in.ReportedAt,
				SourceID:    &in.SourceID,
				EventID:     &in.EventID,
			})
			if isNoRows(err) {
				continue // already ingested
			}
			if err != nil {
				return fmt.Errorf("insert incident %s/%s: %w", in.SourceID, in.EventID, err)
			}
			res.NewIncidents = append(res.NewIncidents, incidentFromRow(row))
		}
		return nil
	})
	if err != nil {
		return IngestResult{}, err
	}
	// Only cache after commit: a rolled-back tx also rolls back the DDL.
	for _, day := range newDays {
		s.partitions.Store(day, struct{}{})
	}
	return res, nil
}

func (s *Store) uncachedDays(samples []SampleRow) []time.Time {
	var days []time.Time
	seen := map[time.Time]bool{}
	for _, r := range samples {
		day := r.TS.UTC().Truncate(24 * time.Hour)
		if seen[day] {
			continue
		}
		seen[day] = true
		if _, ok := s.partitions.Load(day); !ok {
			days = append(days, day)
		}
	}
	return days
}

func sampleColumns(rows []SampleRow) sqlc.InsertMetricSamplesParams {
	n := len(rows)
	p := sqlc.InsertMetricSamplesParams{
		SourceIds: make([]string, n),
		EventIds:  make([]string, n),
		Tss:       make([]time.Time, n),
		VenueIds:  make([]string, n),
		ZoneIds:   make([]string, n),
		Metrics:   make([]string, n),
		Vals:      make([]float64, n),
	}
	for i, r := range rows {
		p.SourceIds[i], p.EventIds[i], p.Tss[i] = r.SourceID, r.EventID, r.TS
		p.VenueIds[i], p.ZoneIds[i], p.Metrics[i], p.Vals[i] = r.VenueID, r.ZoneID, r.Metric, r.Value
	}
	return p
}

func incidentFromRow(r sqlc.Incident) domain.Incident {
	return domain.Incident{
		ID:          r.ID,
		VenueID:     r.VenueID,
		ZoneID:      r.ZoneID,
		Severity:    r.Severity,
		Category:    r.Category,
		Status:      r.Status,
		Description: r.Description,
		ReportedAt:  r.ReportedAt,
		CreatedAt:   r.CreatedAt,
		UpdatedAt:   r.UpdatedAt,
	}
}
