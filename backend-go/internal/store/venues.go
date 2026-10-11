package store

import (
	"context"
	"fmt"
	"math"

	"github.com/ddejongh/venuepulse/backend-go/internal/store/sqlc"
)

type VenueSpec struct {
	ID       string
	Name     string
	Kind     string
	Timezone string
	Zones    []ZoneSpec
}

type ZoneSpec struct {
	ID       string
	Name     string
	Kind     string
	Capacity int // 0 = unknown
}

// UpsertVenue creates or updates a venue and its zones in one transaction.
// Zones absent from spec are left untouched.
func (s *Store) UpsertVenue(ctx context.Context, v VenueSpec) error {
	return s.inTx(ctx, func(q *sqlc.Queries) error {
		tz := v.Timezone
		if tz == "" {
			tz = "UTC"
		}
		if err := q.UpsertVenue(ctx, sqlc.UpsertVenueParams{ID: v.ID, Name: v.Name, Kind: v.Kind, Timezone: tz}); err != nil {
			return fmt.Errorf("upsert venue %s: %w", v.ID, err)
		}
		for _, z := range v.Zones {
			var capacity *int32
			if z.Capacity > 0 {
				if z.Capacity > math.MaxInt32 {
					return fmt.Errorf("zone %s: capacity %d out of range", z.ID, z.Capacity)
				}
				c := int32(z.Capacity)
				capacity = &c
			}
			name := z.Name
			if name == "" {
				name = z.ID
			}
			err := q.UpsertZone(ctx, sqlc.UpsertZoneParams{ID: z.ID, VenueID: v.ID, Name: name, Kind: z.Kind, Capacity: capacity})
			if err != nil {
				return fmt.Errorf("upsert zone %s: %w", z.ID, err)
			}
		}
		return nil
	})
}

// ZoneIndex maps venue ID to the set of its zone IDs.
type ZoneIndex map[string]map[string]struct{}

func (ix ZoneIndex) Has(venueID, zoneID string) bool {
	_, ok := ix[venueID][zoneID]
	return ok
}

// ZoneIndex loads every venue's zone IDs. Venues without zones are omitted.
func (s *Store) ZoneIndex(ctx context.Context) (ZoneIndex, error) {
	rows, err := s.q.ListZoneRefs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list zones: %w", err)
	}
	ix := make(ZoneIndex)
	for _, r := range rows {
		if ix[r.VenueID] == nil {
			ix[r.VenueID] = make(map[string]struct{})
		}
		ix[r.VenueID][r.ID] = struct{}{}
	}
	return ix, nil
}
