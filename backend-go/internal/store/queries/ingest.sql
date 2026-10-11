-- name: EnsureMetricSamplesPartition :exec
SELECT ensure_metric_samples_partition(sqlc.arg(day)::date);

-- name: InsertMetricSamples :execrows
-- Parallel arrays; set-returning functions in a select list are zipped
-- row-wise. (sqlc does not understand multi-argument unnest.)
INSERT INTO metric_samples (source_id, event_id, ts, venue_id, zone_id, metric, value)
SELECT unnest(sqlc.arg(source_ids)::text[]),
       unnest(sqlc.arg(event_ids)::text[]),
       unnest(sqlc.arg(tss)::timestamptz[]),
       unnest(sqlc.arg(venue_ids)::text[]),
       unnest(sqlc.arg(zone_ids)::text[]),
       unnest(sqlc.arg(metrics)::text[]),
       unnest(sqlc.arg(vals)::double precision[])
ON CONFLICT DO NOTHING;

-- name: InsertIncidentFromEvent :one
-- Returns no row when the event was already ingested.
INSERT INTO incidents (venue_id, zone_id, severity, category, description, reported_at, source_id, event_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (source_id, event_id) DO NOTHING
RETURNING *;
