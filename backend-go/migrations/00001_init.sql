-- +goose Up
CREATE TABLE venues (
    id         text PRIMARY KEY,
    name       text NOT NULL,
    kind       text NOT NULL DEFAULT '',
    timezone   text NOT NULL DEFAULT 'UTC',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE zones (
    id             text PRIMARY KEY,
    venue_id       text NOT NULL REFERENCES venues (id) ON DELETE CASCADE,
    parent_zone_id text REFERENCES zones (id) ON DELETE SET NULL,
    name           text NOT NULL,
    kind           text NOT NULL DEFAULT '',
    capacity       integer CHECK (capacity >= 0),
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX zones_venue_id_idx ON zones (venue_id);

-- High-volume time series, range-partitioned by UTC day. No foreign keys:
-- ingest validates venue/zone against its cache before inserting.
-- (source_id, event_id) is the ingest idempotency key; ts is part of the
-- primary key because Postgres requires the partition key in unique indexes.
CREATE TABLE metric_samples (
    source_id   text NOT NULL,
    event_id    text NOT NULL,
    ts          timestamptz NOT NULL,
    venue_id    text NOT NULL,
    zone_id     text NOT NULL,
    metric      text NOT NULL,
    value       double precision NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, event_id, ts)
) PARTITION BY RANGE (ts);
CREATE INDEX metric_samples_zone_metric_ts_idx ON metric_samples (zone_id, metric, ts DESC);

-- Creates the daily partition containing `day` if it does not exist. Serialised
-- with an advisory lock so concurrent ingest workers cannot race.
-- +goose StatementBegin
CREATE FUNCTION ensure_metric_samples_partition(day date) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    part text := format('metric_samples_%s', to_char(day, 'YYYYMMDD'));
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('ensure_metric_samples_partition'));
    IF to_regclass(part) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF metric_samples FOR VALUES FROM (%L) TO (%L)',
            part,
            (day::timestamp AT TIME ZONE 'UTC'),
            ((day + 1)::timestamp AT TIME ZONE 'UTC'));
    END IF;
END;
$$;
-- +goose StatementEnd

CREATE TABLE incidents (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    venue_id    text NOT NULL REFERENCES venues (id) ON DELETE CASCADE,
    zone_id     text NOT NULL REFERENCES zones (id) ON DELETE CASCADE,
    severity    text NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    category    text NOT NULL,
    status      text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'acknowledged', 'resolved')),
    description text NOT NULL DEFAULT '',
    reported_at timestamptz NOT NULL,
    -- Set when the incident came from an ingest event; NULL when created by a user.
    source_id   text,
    event_id    text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_id, event_id)
);
CREATE INDEX incidents_venue_status_idx ON incidents (venue_id, status, reported_at DESC);

-- +goose Down
DROP TABLE incidents;
DROP FUNCTION ensure_metric_samples_partition(date);
DROP TABLE metric_samples;
DROP TABLE zones;
DROP TABLE venues;
