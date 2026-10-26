"""Part C: transactional, idempotent DuckDB storage and dashboard queries.

Use one EventStore shared by subscriber and dashboard in the same process.
Every connection operation is protected by a lock. Offline replay is explicit;
it is useful for testing, but is not evidence of MQTT delivery.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
from datetime import datetime
from pathlib import Path
from threading import RLock

import duckdb
import pandas as pd

import s00_config as config
import s01_messages as messages


class EventStore:
    def __init__(self, db_path=None, stations_path=None, a1_path=None):
        self._lock = RLock()
        self.db_path = Path(db_path or config.A2_DB_PATH)
        source_path = Path(a1_path or config.A1_DB_PATH)
        if self.db_path.resolve() == source_path.resolve():
            raise ValueError('the A2 database must be separate from the A1 source')
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.db_path.exists():
            shutil.copy2(source_path, self.db_path)
        self._con = duckdb.connect(str(self.db_path))
        try:
            self.init_schema(stations_path or config.STATIONS_INPUT_CSV)
        except Exception:
            self._con.close()
            raise

    def init_schema(self, stations_path=None):
        """Load a stable, validated mapping; reject remapping existing station IDs."""
        with open(stations_path or config.STATIONS_INPUT_CSV, newline='', encoding='utf-8-sig') as f:
            rows = list(csv.DictReader(f))
        mapping = pd.DataFrame([
            {'station_id': r['station_id'], 'source_record_id': r['source_record_id'],
             'name': r['name'], 'connectors': int(r['connectors'])} for r in rows
        ])
        if mapping.empty or mapping.station_id.duplicated().any() or mapping.source_record_id.duplicated().any():
            raise ValueError('station mapping must be nonempty and one-to-one')
        with self._lock:
            self._con.register('_mapping_input', mapping)
            self._con.execute('BEGIN')
            try:
                self._con.execute((config.SQL_DIR / 'schema_a2.sql').read_text(encoding='utf-8-sig'))
                missing = self._con.execute('''SELECT m.source_record_id FROM _mapping_input m
                    ANTI JOIN ev_chargers c USING (source_record_id)''').fetchall()
                if missing:
                    raise ValueError(f'mapping refers to missing A1 records: {missing[:3]}')
                changed = self._con.execute('''SELECT m.station_id FROM _mapping_input m
                    JOIN station_mapping s USING (station_id)
                    WHERE m.source_record_id != s.source_record_id OR m.connectors != s.connectors''').fetchall()
                if changed:
                    raise ValueError('existing station mapping changed; use a separate database for the new dataset')
                self._con.execute('INSERT INTO station_mapping SELECT * FROM _mapping_input ON CONFLICT DO NOTHING')
                self._con.execute('COMMIT')
            except Exception:
                self._con.execute('ROLLBACK')
                raise
            finally:
                self._con.unregister('_mapping_input')
            self._connectors = dict(self._con.execute('SELECT station_id, connectors FROM station_mapping').fetchall())

    def _validate(self, event):
        required = messages.ENVELOPE + ['seq'] + messages.field_names(event.get('event_type'), event.get('status'))
        missing = set(required) - event.keys()
        if missing:
            raise ValueError(f'missing event fields: {sorted(missing)}')
        for name in ('event_id', 'station_id'):
            if not isinstance(event[name], str) or not event[name]:
                raise ValueError(f'{name} must be a nonempty string')
        if event['station_id'] not in self._connectors:
            raise ValueError(f"unmapped station: {event['station_id']}")
        if type(event['seq']) is not int or event['seq'] < 0:
            raise ValueError('seq must be a nonnegative integer')
        for key in ('event_ts', 'charge_end_ts'):
            if key in event:
                ts = datetime.fromisoformat(event[key])
                if ts.tzinfo is not None:
                    raise ValueError('expected generator local timestamps without timezone')
        if event['event_type'] != 'price_update':
            if not isinstance(event['session_id'], str) or not event['session_id']:
                raise ValueError('session_id must be a nonempty string')
            connector = event['connector_id']
            if type(connector) is not int or not 1 <= connector <= self._connectors[event['station_id']]:
                raise ValueError('connector_id outside station connector range')
        for name in set(required) & messages.FLOAT_FIELDS:
            value = event[name]
            if value is None and name == 'prev_price_per_kwh':
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(f'{name} must be finite numeric data')
        return json.dumps(event, sort_keys=True, separators=(',', ':'), allow_nan=False)

    def insert_event(self, event):
        """B's callback interface. True if inserted, False for an identical replay."""
        return self.insert_events([event]) == 1

    def insert_events(self, events):
        """Atomically insert a batch. A malformed event rolls back the entire batch.

        Deduplication survives restarts. Reusing an ID with different content is an
        error, so raw history can never silently disagree with derived tables.
        """
        rows = {}
        for event in events:
            payload = self._validate(event)
            row = (event['event_id'], event['seq'], event['event_type'], event['event_ts'], event['station_id'], payload)
            if event['event_id'] in rows and rows[event['event_id']] != row:
                raise ValueError('conflicting payloads for the same event_id')
            rows[event['event_id']] = row
        if not rows:
            return 0
        frame = pd.DataFrame(rows.values(), columns=['event_id', 'seq', 'event_type', 'event_ts', 'station_id', 'payload'])
        with self._lock:
            self._con.register('_incoming', frame)
            self._con.execute('BEGIN')
            try:
                conflict = self._con.execute('''SELECT i.event_id FROM _incoming i JOIN raw_events r USING(event_id)
                    WHERE i.payload != CAST(r.payload AS VARCHAR) LIMIT 1''').fetchone()
                if conflict:
                    raise ValueError(f'event_id already stored with different content: {conflict[0]}')
                self._con.execute('''CREATE OR REPLACE TEMP TABLE _new_events AS
                    SELECT i.event_id, i.seq, i.event_type, CAST(i.event_ts AS TIMESTAMP) AS event_ts,
                           i.station_id, CAST(i.payload AS JSON) AS payload
                    FROM _incoming i ANTI JOIN raw_events r USING(event_id)
                    ORDER BY i.event_ts, i.seq''')
                count = self._con.execute('SELECT count(*) FROM _new_events').fetchone()[0]
                identity_conflict = self._con.execute('''WITH identities AS (
                    SELECT session_id, station_id, connector_id FROM charging_sessions
                    UNION ALL
                    SELECT (payload->>'session_id'), station_id, CAST((payload->>'connector_id') AS INTEGER)
                    FROM _new_events WHERE event_type IN ('session','meter'))
                    SELECT session_id FROM identities GROUP BY session_id
                    HAVING count(DISTINCT (station_id, connector_id)) > 1 LIMIT 1''').fetchone()
                if identity_conflict:
                    raise ValueError('session_id belongs to different station/connector')
                self._con.execute('''INSERT INTO raw_events(event_id, seq, event_type, event_ts, station_id, payload)
                    SELECT * FROM _new_events''')
                self._update_tables()
                self._con.execute('COMMIT')
                return count
            except Exception:
                self._con.execute('ROLLBACK')
                raise
            finally:
                self._con.unregister('_incoming')

    def _update_tables(self):
        self._con.execute('''INSERT INTO price_updates
            SELECT event_id, station_id, event_ts, seq,
                   CAST((payload->>'price_per_kwh') AS DOUBLE), CAST((payload->>'wholesale_per_kwh') AS DOUBLE)
            FROM _new_events WHERE event_type = 'price_update' ''')
        # Even a meter/end without a start creates a session. Completed sessions
        # are never reactivated by a late start or meter message.
        self._con.execute('''INSERT INTO charging_sessions(session_id, station_id, connector_id, status)
            SELECT DISTINCT (payload->>'session_id'), station_id, CAST((payload->>'connector_id') AS INTEGER), 'active'
            FROM _new_events WHERE event_type IN ('session','meter') ON CONFLICT DO NOTHING''')
        self._con.execute('''UPDATE charging_sessions AS s SET
                start_ts = e.event_ts, vehicle_model = (e.payload->>'vehicle_model')
            FROM _new_events e WHERE e.event_type = 'session' AND (e.payload->>'status') = 'start'
              AND s.session_id = (e.payload->>'session_id') ''')
        self._con.execute('''UPDATE charging_sessions AS s SET
                end_ts = e.event_ts, status = 'completed',
                charge_end_ts = CAST((e.payload->>'charge_end_ts') AS TIMESTAMP),
                charge_minutes = CAST((e.payload->>'charge_minutes') AS DOUBLE),
                energy_kwh = CAST((e.payload->>'energy_kwh') AS DOUBLE),
                avg_kw = CAST((e.payload->>'avg_kw') AS DOUBLE),
                peak_kw = CAST((e.payload->>'peak_kw') AS DOUBLE),
                price_per_kwh = CAST((e.payload->>'price_per_kwh') AS DOUBLE),
                cost_aud = CAST((e.payload->>'cost_aud') AS DOUBLE)
            FROM _new_events e WHERE e.event_type = 'session' AND (e.payload->>'status') = 'end'
              AND s.session_id = (e.payload->>'session_id') ''')
        self._con.execute('''INSERT INTO meter_readings
            SELECT event_id, (payload->>'session_id'), station_id, CAST((payload->>'connector_id') AS INTEGER),
                   event_ts, seq, CAST((payload->>'voltage_v') AS DOUBLE), CAST((payload->>'current_a') AS DOUBLE),
                   CAST((payload->>'power_kw') AS DOUBLE), CAST((payload->>'energy_kwh') AS DOUBLE),
                   CAST((payload->>'soc_pct') AS DOUBLE)
            FROM _new_events WHERE event_type = 'meter' ''')
        invalid = self._con.execute('''SELECT session_id FROM charging_sessions
            WHERE end_ts < start_ts OR charge_end_ts > end_ts OR charge_end_ts < start_ts LIMIT 1''').fetchone()
        if invalid:
            raise ValueError(f'invalid session timestamps: {invalid[0]}')

    def _query(self, sql, params=None):
        with self._lock:
            result = self._con.execute(sql, params or [])
            names = [c[0] for c in result.description]
            return [dict(zip(names, row)) for row in result.fetchall()]

    def charger_details(self):
        """Dictionary keyed by station_id; static details for D's map markers."""
        return {r['station_id']: r for r in self._query('SELECT * FROM v_charger_details ORDER BY station_id')}

    def current_metrics(self):
        """Current means latest observed event state, not the computer wall clock."""
        return self._query('''SELECT
            (SELECT count(*) FROM charging_sessions WHERE status='active') AS active_sessions,
            (SELECT count(DISTINCT (station_id, connector_id)) FROM charging_sessions WHERE status='active') AS occupied_connectors,
            (SELECT count(DISTINCT station_id) FROM charging_sessions WHERE status='active') AS active_stations,
            (SELECT count(*) FROM v_latest_price) AS priced_stations,
            (SELECT max(event_ts) FROM raw_events) AS latest_event_ts''')[0]

    def historical_metrics(self):
        return self._query('''SELECT count(*) AS completed_sessions,
            coalesce(sum(energy_kwh),0) AS energy_kwh, coalesce(sum(cost_aud),0) AS revenue_aud,
            avg(date_diff('second',start_ts,end_ts)/60.0) AS avg_occupied_minutes,
            avg(charge_minutes) AS avg_charge_minutes,
            count(*) FILTER (WHERE start_ts IS NULL) AS sessions_missing_start,
            (SELECT count(*) FROM raw_events) AS total_events
            FROM charging_sessions WHERE status='completed' ''')[0]

    def latest_prices(self):
        return self._query('SELECT * FROM v_latest_price ORDER BY station_id')

    def active_sessions(self):
        return self._query('SELECT * FROM v_active_sessions ORDER BY station_id, session_id')

    def station_usage(self, station_id=None):
        where = ' WHERE station_id = ?' if station_id is not None else ''
        return self._query('SELECT * FROM v_station_usage' + where + ' ORDER BY usage_date, station_id',
                           [station_id] if station_id is not None else [])

    def session_history(self, station_id):
        return self._query('SELECT * FROM charging_sessions WHERE station_id=? ORDER BY start_ts, session_id', [station_id])

    def meter_history(self, session_id):
        return self._query('SELECT * FROM meter_readings WHERE session_id=? ORDER BY event_ts, seq', [session_id])

    def close(self):
        with self._lock:
            self._con.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', type=Path, default=config.A2_DB_PATH)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--replay', type=Path, help='offline JSONL replay; omit to only initialize/query')
    mode.add_argument('--subscribe', action='store_true', help='receive MQTT through B and store events until Ctrl+C')
    args = parser.parse_args()
    with EventStore(args.db) as store:
        if args.subscribe:
            from s06_subscriber import EventSubscriber
            subscriber = EventSubscriber(store=store)
            try:
                subscriber.client.connect(config.BROKER_HOST, config.BROKER_PORT, 60)
                subscriber.client.loop_forever()
            except KeyboardInterrupt:
                print(f'Processed {subscriber.processed} MQTT events; errors: {subscriber.errors}')
            finally:
                subscriber.client.disconnect()
        if args.replay:
            inserted = 0
            batch = []
            with args.replay.open(encoding='utf-8') as f:
                for line in f:
                    if not line.strip():
                        continue
                    batch.append(json.loads(line))
                    if len(batch) == 1000:
                        inserted += store.insert_events(batch)
                        batch.clear()
                inserted += store.insert_events(batch)
            print(f'Offline replay: inserted {inserted} new events (not MQTT receipt).')
        print(json.dumps({'current': store.current_metrics(), 'historical': store.historical_metrics()}, default=str, indent=2))


if __name__ == '__main__':
    main()
