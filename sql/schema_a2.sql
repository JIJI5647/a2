-- A2 additions only. A1 tables are never modified.
-- PK indexes enforce identities/deduplication; no speculative secondary indexes.
CREATE TABLE IF NOT EXISTS station_mapping (
    station_id VARCHAR PRIMARY KEY,
    source_record_id VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL,
    connectors INTEGER NOT NULL CHECK (connectors > 0)
);

CREATE TABLE IF NOT EXISTS raw_events (
    event_id VARCHAR PRIMARY KEY,
    seq BIGINT NOT NULL CHECK (seq >= 0),
    event_type VARCHAR NOT NULL CHECK (event_type IN ('price_update','session','meter')),
    event_ts TIMESTAMP NOT NULL,
    received_ts TIMESTAMP NOT NULL DEFAULT current_timestamp,
    station_id VARCHAR NOT NULL,
    payload JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS price_updates (
    event_id VARCHAR PRIMARY KEY,
    station_id VARCHAR NOT NULL,
    event_ts TIMESTAMP NOT NULL,
    seq BIGINT NOT NULL,
    price_per_kwh DOUBLE NOT NULL,
    wholesale_per_kwh DOUBLE
);

-- start_ts may be unknown when subscribing mid-stream. A late start fills it in.
CREATE TABLE IF NOT EXISTS charging_sessions (
    session_id VARCHAR PRIMARY KEY,
    station_id VARCHAR NOT NULL,
    connector_id INTEGER NOT NULL CHECK (connector_id > 0),
    vehicle_model VARCHAR,
    start_ts TIMESTAMP,
    charge_end_ts TIMESTAMP,
    end_ts TIMESTAMP,
    status VARCHAR NOT NULL CHECK (status IN ('active','completed')),
    charge_minutes DOUBLE,
    energy_kwh DOUBLE,
    avg_kw DOUBLE,
    peak_kw DOUBLE,
    price_per_kwh DOUBLE,
    cost_aud DOUBLE
);

CREATE TABLE IF NOT EXISTS meter_readings (
    event_id VARCHAR PRIMARY KEY,
    session_id VARCHAR NOT NULL,
    station_id VARCHAR NOT NULL,
    connector_id INTEGER NOT NULL,
    event_ts TIMESTAMP NOT NULL,
    seq BIGINT NOT NULL,
    voltage_v DOUBLE,
    current_a DOUBLE,
    power_kw DOUBLE,
    energy_kwh DOUBLE,
    soc_pct DOUBLE
);

CREATE OR REPLACE VIEW v_charger_details AS
SELECT m.station_id, m.source_record_id, m.name, m.connectors,
       c.Station_address AS address, c.Operator AS operator,
       c.external_dc_plug_types AS plug_types, c.Charger_rating AS charger_rating,
       c.Latitude AS lat, c.Longitude AS lon, c.SA4_NAME26 AS sa4_name
FROM station_mapping m JOIN ev_chargers c USING (source_record_id);

CREATE OR REPLACE VIEW v_active_sessions AS
SELECT s.*, c.name, c.operator, c.address, c.lat, c.lon
FROM charging_sessions s JOIN v_charger_details c USING (station_id)
WHERE s.status = 'active';

CREATE OR REPLACE VIEW v_latest_price AS
SELECT station_id, price_per_kwh, event_ts AS price_ts
FROM price_updates
QUALIFY row_number() OVER (PARTITION BY station_id ORDER BY event_ts DESC, seq DESC, event_id DESC) = 1;

-- Historical totals are attributed to the unplug date, not split across midnight.
-- Missing starts remain NULL in duration averages, rather than becoming zero.
CREATE OR REPLACE VIEW v_station_usage AS
SELECT s.station_id, CAST(s.end_ts AS DATE) AS usage_date,
       c.operator, c.sa4_name,
       count(*) AS completed_sessions,
       sum(s.energy_kwh) AS energy_kwh, sum(s.cost_aud) AS revenue_aud,
       avg(date_diff('second', s.start_ts, s.end_ts) / 60.0) AS avg_occupied_minutes,
       avg(s.charge_minutes) AS avg_charge_minutes
FROM charging_sessions s JOIN v_charger_details c USING (station_id)
WHERE s.status = 'completed'
GROUP BY s.station_id, CAST(s.end_ts AS DATE), c.operator, c.sa4_name;
