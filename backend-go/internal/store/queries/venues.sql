-- name: UpsertVenue :exec
INSERT INTO venues (id, name, kind, timezone)
VALUES ($1, $2, $3, $4)
ON CONFLICT (id) DO UPDATE
SET name = excluded.name,
    kind = excluded.kind,
    timezone = excluded.timezone,
    updated_at = now();

-- name: UpsertZone :exec
INSERT INTO zones (id, venue_id, name, kind, capacity)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (id) DO UPDATE
SET venue_id = excluded.venue_id,
    name = excluded.name,
    kind = excluded.kind,
    capacity = excluded.capacity,
    updated_at = now();

-- name: ListZoneRefs :many
SELECT venue_id, id FROM zones ORDER BY venue_id, id;
