-- Assignment 2 schema extension for the copied A1 database (db/ev_chargers_a2.duckdb).
-- Idempotent: safe to run on every start (EventStore.init_schema).
-- A1 tables (ev_chargers, sa4_regions, ...) are kept unchanged; new tables reference
-- ev_chargers.source_record_id through station_id.
--
-- TODO (Task 4 / report "schema changes and physical design optimisations"):
--   - review column types and NOT NULL / CHECK constraints
--   - decide on FOREIGN KEY to ev_chargers(source_record_id) (DuckDB FKs slow inserts;
--     alternatively validate in the subscriber and document the choice)
--   - indexes: DuckDB is a column store with zone maps (W10 p12); ART indexes mainly
--     help point lookups / PK checks. Justify each index you keep.
--   - consider inserting in event-time order so zone maps on event_ts prune well

-- 1. Raw landing table: every received message, append-only (point-based, W7 p36-37)
CREATE TABLE IF NOT EXISTS raw_events (
    event_id      VARCHAR PRIMARY KEY,     -- dedup under QoS 1 (at-least-once)
    seq           BIGINT NOT NULL,
    event_type    VARCHAR NOT NULL CHECK (event_type IN ('price_update','session_start','session_end','meter')),
    event_ts      TIMESTAMP NOT NULL,      -- event time
    received_ts   TIMESTAMP NOT NULL DEFAULT current_timestamp,  -- processing time (W9 p33)
    station_id    VARCHAR NOT NULL,
    payload       JSON NOT NULL
);

-- 2. Price history per station
CREATE TABLE IF NOT EXISTS price_updates (
    station_id         VARCHAR NOT NULL,
    event_ts           TIMESTAMP NOT NULL,
    price_per_kwh      DECIMAL(6,2) NOT NULL,
    wholesale_per_kwh  DECIMAL(8,4),
    PRIMARY KEY (station_id, event_ts)
);

-- 3. One row per charging session; status changes active -> completed
CREATE TABLE IF NOT EXISTS charging_sessions (
    session_id      VARCHAR PRIMARY KEY,
    station_id      VARCHAR NOT NULL,
    connector_id    INTEGER NOT NULL,
    vehicle_model   VARCHAR,
    start_ts        TIMESTAMP NOT NULL,
    charge_end_ts   TIMESTAMP,
    end_ts          TIMESTAMP,
    status          VARCHAR NOT NULL CHECK (status IN ('active','completed')),
    energy_kwh      DOUBLE,
    avg_kw          DOUBLE,
    peak_kw         DOUBLE,
    price_per_kwh   DOUBLE,
    cost_aud        DOUBLE
);

-- 4. Meter telemetry (largest table)
CREATE TABLE IF NOT EXISTS meter_readings (
    session_id    VARCHAR NOT NULL,
    station_id    VARCHAR NOT NULL,
    connector_id  INTEGER NOT NULL,
    event_ts      TIMESTAMP NOT NULL,
    voltage_v     DOUBLE,
    current_a     DOUBLE,
    power_kw      DOUBLE,
    energy_kwh    DOUBLE,
    soc_pct       DOUBLE
);

-- 5. Views for the dashboard (TODO: complete / add more)
CREATE OR REPLACE VIEW v_active_sessions AS
SELECT s.*, c.Operator, c.Station_address, c.Latitude, c.Longitude
FROM charging_sessions s
JOIN ev_chargers c ON c.source_record_id = s.station_id
WHERE s.status = 'active';

CREATE OR REPLACE VIEW v_latest_price AS
SELECT station_id, arg_max(price_per_kwh, event_ts) AS price_per_kwh, max(event_ts) AS price_ts
FROM price_updates
GROUP BY station_id;

-- TODO: v_station_usage (sessions, kWh, revenue, utilisation per station/day),
--       usage by Operator or SA4_NAME26 via ev_chargers
