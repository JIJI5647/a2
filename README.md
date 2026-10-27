# COMP5339 Assignment 2 – EV Charging Data Streaming and Analysis

Simulated real-time EV charger prices and charging sessions for our Assignment 1
NSW DC chargers: generated → published via MQTT → subscribed → stored in DuckDB →
shown on a live map dashboard.

> Status: data generation and MQTT publishing (s00–s05) work end to end.
> B: s06/s07 now contain subscriber deduplication and live-state processing.
> C: s08 and the A2 schema are implemented and tested with the complete offline event stream.
> D: s09 dashboard callbacks are still skeletons. Live MQTT-to-database delivery still needs integration testing.

## Project structure

```text
a2/
├── COMP5339_2026s2_Assignment2_data _generator/  # provided generator (unchanged)
├── data/
│   ├── a1/         # ev_chargers.duckdb: Assignment 1 database (unchanged copy, read-only)
│   ├── raw/        # s02-s03: stations_input.csv + generator outputs
│   └── events/     # s04: event_stream.jsonl (ordered MQTT messages)
├── db/             # ev_chargers_a2.duckdb (copy of A1 db + A2 tables)
├── sql/
│   └── schema_a2.sql   # DDL for the A2 tables and views
├── src/                           # files run / are read in this order
│   ├── s00_config.py              # [shared]   paths, generator + MQTT settings
│   ├── s01_messages.py            # [shared/A] JSON message format
│   ├── s02_export_stations.py     # [A] A1 DuckDB -> generator station CSV, copy db (Task 1)
│   ├── s03_generate_dataset.py    # [A] run provided generator (Task 1)
│   ├── s04_build_event_stream.py  # [A] de-dup prices, split sessions, event-time order (Task 2)
│   ├── s05_publish_events.py      # [A] MQTT publisher (Task 2)
│   ├── s06_subscriber.py          # [B] MQTT subscriber (Task 3)
│   ├── s07_live_state.py          # [B] in-memory current state (Task 3)
│   ├── s08_event_store.py         # [C] DuckDB storage + analytics (Task 4)
│   └── s09_dashboard.py           # [D] Dash + Leaflet map dashboard (Task 3/4)
├── requirements.txt
└── README.md
```

## Setup

Python 3.10+. Run all commands from the `a2/` directory.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 1. Data generation (s02 → s03 → s04)

```bash
python src/s02_export_stations.py
python src/s03_generate_dataset.py
python src/s04_build_event_stream.py
```

Each step overwrites its own output. With unchanged parameters the result is identical
every time (fixed seed). The database copy in `db/` is created once and then kept.

### s02 – A1 chargers → generator input

Reads the 432 DC chargers (view `dc_chargers`) from the A1 database
(`data/a1/ev_chargers.duckdb`) and writes
`data/raw/stations_input.csv` in the generator's format:

| Column | Rule |
|---|---|
| `station_id` | first 3 letters of the LGA in upper case + running number, e.g. `KUR-04`; `UNK-nn` without LGA |
| `name` | `Station_name` is NULL for 431/432 DC chargers → `"<Operator> - <LGA>"` |
| `lat`, `lon` | `Latitude`, `Longitude` |
| `site_type` | `urban` if the SA4 is in GCC *Greater Sydney* (251), else `regional` (181) |
| `connectors` | `Number_of_plugs` (no cap) |
| `max_kw` | `Charger_rating` `"50 kW"` → 50; mixed `"2x350kW & 2x175kW"` → highest value (350) |
| `source_record_id` | A1 primary key, kept to join events back to `ev_chargers` |

### s03 – run the provided generator

Calls `generate_dataset.py` (unchanged) with the parameters from `s00_config.py`.
It downloads real AEMO NSW wholesale prices and simulates per station:

- **hourly price** = station base price + wholesale price deviation + $0.10 from 4–9 pm
- **sessions**: Poisson arrivals per connector (urban 4/day, regional 3/day, mid-day peak,
  fewer at high prices); a driver leaves if the connector is busy
- **charging**: random vehicle from an AU fleet mix, minute-by-minute power limited by
  charger, vehicle and battery taper; a meter reading every `GEN_METER_EVERY` minutes

Output in `data/raw/` (current parameters):

| File | Rows | One row = |
|---|---|---|
| `prices_hourly.csv` | 10,368 | price of one station in one hour |
| `sessions.csv` | 5,170 | one charging session (start, charge end, unplug, kWh, cost) |
| `session_metering.csv` | 29,247 | one meter reading of a session |
| `stations.csv` | 432 | station list + generator base price |
| `aemo_cache/` | – | downloaded AEMO prices |

### s04 – build the ordered event stream

Turns the three CSVs into `data/events/event_stream.jsonl`, one MQTT message per line:

1. **prices**: per station, only publish when the price differs from the previous hour
   (10,368 → 7,344; the first price of each station is always published)
2. **sessions**: each row becomes two `session` events – `start` at `start_ts`
   (only what is known at plug-in) and `end` at `end_ts` (unplug, with kWh and cost)
3. **meter**: one event per meter reading
4. **order**: sort by event time; ties: price → session start → meter → session end;
   then number the events `seq = 0..n-1`

Result: **46,931 events** (7,344 price, 5,170 start, 29,247 meter, 5,170 end),
event time 2026-10-01 00:00 to 2026-10-02 00:55.

## 2. Parameters

### Generation – `src/s00_config.py`

| Parameter | Value | Meaning |
|---|---|---|
| `GEN_START` | `2026-10-01` | first simulated day (must be in the past: real AEMO prices) |
| `GEN_DAYS` | `1` | number of simulated days |
| `GEN_SEED` | `5339` | fixed random seed |
| `GEN_METER_EVERY` | `5` | minutes between meter readings |

After changing one of these, rerun s03 and s04.

### MQTT – environment variables

| Variable | Default | Meaning |
|---|---|---|
| `A2_TOPIC_ROOT` | `UNIKEY/ass2` | topic prefix; **set a unique one**, e.g. `<unikey>/ass2` |
| `A2_BROKER_HOST` | `broker.hivemq.com` | broker (public, as in the Week 9 tutorial); `localhost` for Mosquitto |
| `A2_BROKER_PORT` | `1883` | broker port |
| `A2_QOS` | `1` | MQTT QoS (at least once) |

### Publisher – `src/s05_publish_events.py`

| Option | Default | Meaning |
|---|---|---|
| `--max-events N` | 500 (`DEV_MAX_EVENTS`) | stop after N events |
| `--all` | off | publish the whole stream (~1.4 h) |
| `--start-seq N` | 0 | start at event N (skip ahead, e.g. for the demo) |
| `--no-catch-up` | off | with `--start-seq`: do not re-send the state first (plain resume after a failure) |
| `--delay S` | 0.1 | seconds between messages (must be ≥ 0.1) |

## 3. Publishing (s05) and receiving (s06)

### Topics and messages

| Topic | `event_type` in the message | Retained |
|---|---|---|
| `<root>/price/<station_id>` | `price_update` | yes – broker keeps the latest price per station |
| `<root>/charging/<station_id>` | `session` (`status` `start`/`end`) and `meter` | no |

Session and meter events share one topic per station because MQTT only keeps order
within a topic. Subscribe to `<root>/#` and read `event_type` from the JSON.
Field lists per type: `FIELDS` in `src/s01_messages.py`. Example (`session` start):

```json
{"event_id": "session:start:KUR-04-1-202610011002", "event_type": "session", "status": "start",
 "event_ts": "2026-10-01T10:02:00", "station_id": "KUR-04", "session_id": "KUR-04-1-202610011002",
 "connector_id": 1, "vehicle_model": "Tesla Model Y", "seq": 10850}
```

### Run

Terminal 1 – start the receiver first and **wait until it prints `connected`**
(messages published before the subscription is active are lost):

```bash
A2_TOPIC_ROOT=<unikey>/ass2 python src/s06_subscriber.py
```

Terminal 2 – publish with the same `A2_TOPIC_ROOT`:

```bash
A2_TOPIC_ROOT=<unikey>/ass2 python src/s05_publish_events.py --start-seq 10756 --max-events 100
```

Useful start points (current data): every full hour begins with 432 price updates.

| `--start-seq` | Starts at |
|---|---|
| 0 | 00:00 prices (night: few sessions) |
| 10324 | 10:00 prices |
| 10756 | 10:00 charging events (after the price burst) |
| 17244 | 12:00 charging events |

Full run (fills the database; keep the receiver/dashboard running for ~1.4 h):

```bash
A2_TOPIC_ROOT=<unikey>/ass2 python src/s05_publish_events.py --all
```

Notes:

- On connect, the receiver first gets the retained prices left by earlier runs on the
  same prefix. Use a fresh prefix for a clean test.
- Starting mid-stream (`--start-seq N`), s05 first re-sends the state at N
  (catch-up): the latest price of every station and the `start` of every session
  still running, as the original messages (same `seq`/`event_id`). The receiver
  then knows all current prices and occupied connectors from the first second.
  At 10756 this is 628 messages (~1 min). `--no-catch-up` skips it.

## Assumptions

- Only DC chargers from A1 are simulated (432 stations, 1,656 connectors).
- `station_id`, `site_type` and `max_kw` follow the s02 rules above.
- Prices follow the generator's hypothetical dynamic tariff (real NRMA prices are flat).
- A session ends at unplug (`end_ts`); the connector is occupied until then.
- 1 simulated day with meter readings every 5 min keeps a full run at ~1.4 h.

## Outputs and data files

| Path | Content |
|---|---|
| `data/a1/ev_chargers.duckdb` | Assignment 1 database, unchanged copy (input of s02) |
| `data/raw/stations_input.csv` | A1 DC chargers in generator format + `source_record_id` |
| `data/raw/*.csv`, `data/raw/aemo_cache/` | generator outputs (see s03) |
| `data/events/event_stream.jsonl` | ordered MQTT messages (see s04) |
| `db/ev_chargers_a2.duckdb` | copy of the A1 database, extended in Task 4 |
| `sql/schema_a2.sql` | DDL for the Task 4 tables |


## 4. Part C: DuckDB storage and analytics

`src/s08_event_store.py` implements `EventStore`, which B calls through
`store.insert_event(event)` and D shares in the same process. The existing A1
source database is never edited. An absent target database is copied from A1;
an existing target is kept. Initialization adds the A2 tables/views and imports
432 station mappings. It does not automatically load synthetic history.

### Run Part C independently (PowerShell)

```powershell
# Initialize the default A2 database and print metrics (no MQTT dependency needed).
python src/s08_event_store.py

# Receiver terminal: use the SAME unique prefix as the publisher.
$env:A2_TOPIC_ROOT = "your_unikey/ass2"
python src/s08_event_store.py --subscribe

# Publisher in another terminal, after the receiver is subscribed:
$env:A2_TOPIC_ROOT = "your_unikey/ass2"
python src/s05_publish_events.py --start-seq 10756 --max-events 100
```

The subscribe command uses B's `EventSubscriber(store=store)` and runs until
Ctrl+C. It needs `paho-mqtt` from requirements.txt. For a complete history, start
at sequence 0 and publish with `--all`. Starting mid-stream with `--no-catch-up`
creates partial sessions with an unknown start time. Do not run the standalone receiver and the
Dash process as separate writers against the same DuckDB file; Dash already
creates a shared store/subscriber in one process.

### Offline replay and verification

```powershell
# Separate offline database, so future live runs do not inherit replayed history.
python src/s08_event_store.py --db db/ev_chargers_offline.duckdb --replay data/events/event_stream.jsonl

# Replay again: zero new events; totals remain unchanged.
python src/s08_event_store.py --db db/ev_chargers_offline.duckdb --replay data/events/event_stream.jsonl

python -B -m unittest discover -s tests -v
```

The replay is a storage test, not proof of MQTT receipt. Tests use temporary copies
inside the project and remove them afterwards. The full stream should produce
46,931 raw events, 7,344 prices, 5,170 completed sessions, 29,247 meter readings,
432 priced stations, and zero active sessions after the final end event. Tests
compare total energy and revenue against `sessions.csv`, verify restart dedup,
late starts, out-of-order prices, rollback, invalid input, and concurrent access.

### Schema, state, and physical design

- `station_mapping` joins synthetic `station_id` to A1 `source_record_id`.
  Each mapping must point to an existing A1 row. Existing IDs cannot be remapped
  to different A1 records or connector counts.
- `raw_events` stores one row per unique event ID, the complete JSON payload,
  event timestamp, sequence, and local processing timestamp (`received_ts`).
  Repeated identical IDs are ignored across restarts; conflicting payloads raise
  an error. MQTT redelivery attempts are not counted as new business events.
- `price_updates`, `charging_sessions`, and `meter_readings` support typed SQL
  queries. Raw and derived changes commit together; a failed event/batch leaves
  neither partially written. The JSON contract stays `session` + `status`.
- `charging_sessions` can begin from a meter or end event. Missing starts are
  NULL, and late starts enrich a completed session without reactivating it.
  Current state means the latest observed stream state, not today's wall clock.
  A missed end can leave a session active; no timeout is guessed.
- Latest prices use event time and sequence, not arrival order. Event timestamps
  retain the generator's naive local-time convention. Use a separate database
  for a regenerated dataset whose events reuse IDs with different content.
- `v_charger_details`, `v_active_sessions`, `v_latest_price`, and `v_station_usage`
  provide mapped static details, active sessions, latest prices, and daily usage.
  Daily completed-session totals are assigned to the unplug date (including
  sessions crossing midnight); this is not daily utilisation. Occupancy duration
  averages exclude missing starts. Charge duration is reported separately.
- Primary-key/unique indexes enforce deduplication and mapping identity.
  No extra secondary indexes are added without a measured query need. Batches
  are inserted in event-time order to help columnar scans. Station references
  and session/connector consistency are checked in EventStore rather than via
  foreign keys on every event table. Direct SQL writes bypass those checks.
- A single `RLock` guards all database reads/writes in the shared process.
  The offline replay commits batches of 1,000; live callbacks commit each event.

### Interface for member D

| Method | Return / purpose |
|---|---|
| `charger_details()` | Dict keyed by station ID; name, address, operator, plug_types, lat/lon, connectors, rating, SA4 and A1 key. Plug types may be unknown (`None`). |
| `current_metrics()` | Dict: active_sessions, occupied_connectors, active_stations, priced_stations, latest_event_ts. |
| `historical_metrics()` | Dict: completed_sessions, energy_kwh, revenue_aud, avg_occupied_minutes, avg_charge_minutes, sessions_missing_start, total_events. |
| `latest_prices()` | List of dicts: station_id, price_per_kwh, price_ts. |
| `active_sessions()` | List of active session dicts enriched with station details. |
| `station_usage(station_id=None)` | Daily completed sessions, energy, revenue, and average durations, with operator and SA4. |
| `session_history(station_id)` | All known sessions for a station, including incomplete sessions. |
| `meter_history(session_id)` | Stored readings ordered by event time and sequence, including completed sessions. |

Query timestamps/dates are Python datetime/date objects. Empty totals are zero;
averages without observations are `None`. D must render at least one current
metric and one historical metric using these APIs; those UI callbacks remain
D's work. B's in-memory LiveState is separate and is not automatically restored
from DuckDB after a process restart.
