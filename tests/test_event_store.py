"""Part C regression tests: isolated copies of A1; never change the live A2 DB."""
import copy
import csv
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import s00_config as config
from s08_event_store import EventStore


class EventStoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.events = [json.loads(line) for line in config.EVENT_STREAM_PATH.read_text().splitlines()]
        cls.start = next(e for e in cls.events if e.get('status') == 'start')
        cls.end = next(e for e in cls.events if e.get('status') == 'end' and e['session_id'] == cls.start['session_id'])
        cls.meter = next(e for e in cls.events if e['event_type'] == 'meter' and e['session_id'] == cls.start['session_id'])
        cls.prices = [e for e in cls.events if e['event_type'] == 'price_update' and e['station_id'] == cls.start['station_id']]

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='event-store-test-', dir=config.PROJECT_ROOT)
        assert Path(self.temp.name).resolve().parent == config.PROJECT_ROOT.resolve()
        self.path = Path(self.temp.name) / 'test.duckdb'
        self.store = EventStore(self.path)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_complete_stream_matches_source_and_restart_dedup(self):
        for i in range(0, len(self.events), 1000):
            self.store.insert_events(self.events[i:i+1000])
        history = self.store.historical_metrics()
        with (config.RAW_DIR / 'sessions.csv').open(newline='') as f:
            sessions = list(csv.DictReader(f))
        self.assertEqual(history['total_events'], len(self.events))
        self.assertEqual(history['completed_sessions'], len(sessions))
        self.assertAlmostEqual(history['energy_kwh'], sum(float(s['energy_kwh']) for s in sessions), places=6)
        self.assertAlmostEqual(history['revenue_aud'], sum(float(s['cost_aud']) for s in sessions), places=6)
        self.assertEqual(self.store.current_metrics()['active_sessions'], 0)
        self.assertEqual(self.store.current_metrics()['priced_stations'], 432)
        self.assertEqual(len(self.store.charger_details()), 432)
        self.assertEqual(len(self.store._query('SELECT * FROM meter_readings')), 29247)
        self.assertEqual(sum(r['completed_sessions'] for r in self.store.station_usage()), len(sessions))
        self.assertAlmostEqual(sum(r['energy_kwh'] for r in self.store.station_usage()), history['energy_kwh'], places=6)
        details = self.store.charger_details()[self.start['station_id']]
        self.assertTrue(details['source_record_id'].startswith('ev_'))
        self.store.close()
        self.store = EventStore(self.path)
        self.assertEqual(self.store.insert_events(self.events), 0)
        self.assertEqual(self.store.historical_metrics(), history)

    def test_missing_start_out_of_order_and_late_start(self):
        self.store.insert_event(self.meter)
        self.assertEqual(self.store.current_metrics()['active_sessions'], 1)
        self.assertIsNone(self.store.active_sessions()[0]['start_ts'])
        self.store.insert_event(self.end)
        self.assertEqual(self.store.historical_metrics()['sessions_missing_start'], 1)
        self.assertEqual(self.store.current_metrics()['active_sessions'], 0)
        self.store.insert_event(self.start)
        self.assertEqual(self.store.current_metrics()['active_sessions'], 0)
        self.assertEqual(self.store.historical_metrics()['sessions_missing_start'], 0)
        self.assertEqual(len(self.store.meter_history(self.start['session_id'])), 1)
        self.assertEqual(self.store.session_history(self.start['station_id'])[0]['vehicle_model'], self.start['vehicle_model'])

    def test_latest_price_uses_event_time_not_arrival(self):
        self.store.insert_event(self.prices[-1])
        self.store.insert_event(self.prices[0])
        self.assertEqual(self.store.latest_prices()[0]['price_per_kwh'], self.prices[-1]['price_per_kwh'])

    def test_duplicate_conflict_and_validation(self):
        self.assertTrue(self.store.insert_event(self.start))
        self.assertFalse(self.store.insert_event(self.start))
        changed = copy.deepcopy(self.start)
        changed['vehicle_model'] = 'different'
        with self.assertRaises(ValueError):
            self.store.insert_events([self.prices[0], changed])
        self.assertEqual(self.store.historical_metrics()['total_events'], 1)
        bad = copy.deepcopy(self.meter)
        bad['station_id'] = 'unknown'
        with self.assertRaises(ValueError):
            self.store.insert_event(bad)
        bad = copy.deepcopy(self.meter)
        bad['power_kw'] = float('nan')
        with self.assertRaises(ValueError):
            self.store.insert_event(bad)
        bad = copy.deepcopy(self.meter)
        bad['connector_id'] = 9999
        with self.assertRaises(ValueError):
            self.store.insert_event(bad)

    def test_sql_failure_rolls_back_all_tables(self):
        self.store.insert_event(self.start)
        bad = copy.deepcopy(self.end)
        bad['event_ts'] = '2026-09-01T00:00:00'
        with self.assertRaises(ValueError):
            self.store.insert_events([self.prices[0], bad])
        self.assertEqual(self.store.historical_metrics()['total_events'], 1)
        self.assertEqual(self.store.current_metrics()['active_sessions'], 1)
        self.assertEqual(self.store.latest_prices(), [])
        self.assertTrue(self.store.insert_event(self.end))

    def test_session_identity_and_concurrent_access(self):
        self.store.insert_event(self.start)
        bad = copy.deepcopy(self.meter)
        bad['station_id'] = next(s for s in self.store.charger_details() if s != self.start['station_id'])
        bad['connector_id'] = 1
        with self.assertRaises(ValueError):
            self.store.insert_event(bad)
        def work(_):
            self.store.insert_event(self.prices[0])
            return self.store.current_metrics()['priced_stations']
        with ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(list(pool.map(work, range(12))), [1] * 12)
        self.assertEqual(self.store.historical_metrics()['total_events'], 2)

    def test_cli_replay_and_source_protection(self):
        with self.assertRaises(ValueError):
            EventStore(config.A1_DB_PATH)
        stream = Path(self.temp.name) / 'sample.jsonl'
        stream.write_text('\n'.join(json.dumps(e) for e in [self.start, self.meter, self.end]), encoding='utf-8')
        target = Path(self.temp.name) / 'cli.duckdb'
        command = [sys.executable, '-B', str(config.PROJECT_ROOT / 'src/s08_event_store.py'),
                   '--db', str(target), '--replay', str(stream)]
        first = subprocess.run(command, check=True, capture_output=True, text=True)
        self.assertIn('inserted 3 new events', first.stdout)
        second = subprocess.run(command, check=True, capture_output=True, text=True)
        self.assertIn('inserted 0 new events', second.stdout)


if __name__ == '__main__':
    unittest.main()
