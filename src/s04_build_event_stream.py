"""s04 · Member A · Task 2 prep: turn the generator CSVs into ONE ordered event stream.

Reads data/raw/{prices_hourly,sessions,session_metering}.csv and writes
data/events/event_stream.jsonl: one JSON event per line (format: src/s01_messages.py),
sorted by event time (Week 9 p33-35: event time vs processing time).

Usage:
    python src/s04_build_event_stream.py
"""
from collections import Counter
from datetime import datetime

import pandas as pd

import s00_config as config
import s01_messages as messages

# Tie-break for events with the same timestamp: a session start comes before its
# first meter reading, and the last meter reading before the session end.
TIE_RANK = {
    ("price_update", None): 0,
    ("session", "start"): 1,
    ("meter", None): 2,
    ("session", "end"): 3,
}


def price_events(prices: pd.DataFrame) -> list[dict]:
    """price_update events: only when a station's price actually changes.

    Each station is compared with its own previous hour; the first price of every
    station is always published (prev_price_per_kwh = None).
    """
    p = prices.sort_values(["station_id", "ts"]).copy()
    p["prev"] = p.groupby("station_id")["price_per_kwh"].shift()
    changed = p[p["prev"].isna() | (p["price_per_kwh"] != p["prev"])]
    return [
        messages.make_event("price_update", r.ts, r.station_id,
                            price_per_kwh=r.price_per_kwh,
                            wholesale_per_kwh=r.wholesale_per_kwh,
                            prev_price_per_kwh=r.prev)
        for r in changed.itertuples(index=False)
    ]


def session_events(sessions: pd.DataFrame) -> list[dict]:
    """Two `session` events per row of sessions.csv.

    status="start" at start_ts with FIELDS["session"]["start"] (only what is known at
    plug-in); status="end" at end_ts (unplug, the connector stays occupied until
    then) with FIELDS["session"]["end"].
    """
    start_fields = messages.FIELDS["session"]["start"]
    end_fields = messages.FIELDS["session"]["end"]
    events = []
    for row in sessions.to_dict("records"):
        events.append(messages.make_event("session", row["start_ts"], row["station_id"],
                                          status="start", **{k: row[k] for k in start_fields}))
        events.append(messages.make_event("session", row["end_ts"], row["station_id"],
                                          status="end", **{k: row[k] for k in end_fields}))
    return events


def meter_events(meters: pd.DataFrame) -> list[dict]:
    """meter events, one per row of session_metering.csv."""
    fields = messages.FIELDS["meter"]
    return [
        messages.make_event("meter", row["ts"], row["station_id"], **{k: row[k] for k in fields})
        for row in meters.to_dict("records")
    ]


def order_stream(events: list[dict]) -> list[dict]:
    """Global event-time order + seq numbers.

    Sort key: (event time, tie rank, station, session) so the order is deterministic.
    """
    def key(e):
        return (datetime.fromisoformat(e["event_ts"]),
                TIE_RANK[(e["event_type"], e.get("status"))],
                e["station_id"],
                e.get("session_id") or "")

    events = sorted(events, key=key)
    for seq, e in enumerate(events):
        e["seq"] = seq
    return events


def main() -> None:
    prices = pd.read_csv(config.RAW_DIR / "prices_hourly.csv")
    sessions = pd.read_csv(config.RAW_DIR / "sessions.csv")
    meters = pd.read_csv(config.RAW_DIR / "session_metering.csv")

    price_evts = price_events(prices)
    events = price_evts + session_events(sessions) + meter_events(meters)
    events = order_stream(events)

    config.EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(config.EVENT_STREAM_PATH, "w") as f:
        for e in events:
            f.write(messages.encode(e).decode("utf-8") + "\n")

    # Summary for the report
    counts = Counter(e["event_type"] + (f"/{e['status']}" if "status" in e else "") for e in events)
    print(f"wrote {len(events)} events -> {config.EVENT_STREAM_PATH}")
    print(f"  price rows {len(prices)}, published {len(price_evts)}, "
          f"dropped {len(prices) - len(price_evts)} unchanged prices")
    for kind in ("price_update", "session/start", "meter", "session/end"):
        print(f"  {kind:<14} {counts[kind]}")
    print(f"  event time {events[0]['event_ts']} .. {events[-1]['event_ts']}")
    print(f"  publishing at {config.MIN_PUBLISH_DELAY_S}s/message takes "
          f"~{len(events) * config.MIN_PUBLISH_DELAY_S / 3600:.1f} h")


if __name__ == "__main__":
    main()
