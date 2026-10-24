"""s01 · Shared (owned by Member A) · Task 2: JSON message formats for the MQTT event stream.

Every message shares an envelope; the remaining fields depend on `event_type`.
Keep field names stable: publisher, subscriber, DuckDB store and dashboard all use them.

Envelope (all events):
    {
      "event_id":   str,   # unique, deterministic (lets the subscriber drop duplicates under QoS 1)
      "event_type": str,   # one of config.EVENT_TYPES: price_update | session | meter
      "status":     str,   # session only: "start" (plug-in) or "end" (unplug)
      "event_ts":   str,   # ISO-8601 event time from the generator (NOT publish time)
      "station_id": str,   # generator station id, mapped to A1 via stations_input.csv
      "seq":        int,   # position in the ordered stream (added by s04_build_event_stream.py)
      ...type-specific fields (FIELDS)...
    }

A session start only carries what is known at plug-in time; energy, duration and
cost are sent with the session end.

Design notes (Week 7 p40): do not use timestamps or ids as JSON field names.
"""
from __future__ import annotations

import json
import math
from typing import Any

import s00_config as config

FIELDS = {
    "price_update": ["price_per_kwh", "wholesale_per_kwh", "prev_price_per_kwh"],
    "session": {
        "start": ["session_id", "connector_id", "vehicle_model"],
        "end": ["session_id", "connector_id", "charge_end_ts", "charge_minutes",
                "energy_kwh", "avg_kw", "peak_kw", "price_per_kwh", "cost_aud"],
    },
    "meter": ["session_id", "connector_id", "voltage_v", "current_a", "power_kw",
              "energy_kwh", "soc_pct"],
}

ENVELOPE = ["event_id", "event_type", "event_ts", "station_id"]

# CSV values arrive as strings or numpy types; cast them to plain JSON types.
INT_FIELDS = {"connector_id"}
FLOAT_FIELDS = {"price_per_kwh", "wholesale_per_kwh", "prev_price_per_kwh", "charge_minutes",
                "energy_kwh", "avg_kw", "peak_kw", "cost_aud", "voltage_v", "current_a",
                "power_kw", "soc_pct"}


def field_names(event_type: str, status: str | None = None) -> list[str]:
    """Type-specific field names; validates the event_type / status combination."""
    if event_type not in config.EVENT_TYPES:
        raise ValueError(f"unknown event_type {event_type!r}")
    if event_type == "session":
        if status not in config.SESSION_STATUSES:
            raise ValueError(f"session needs status in {config.SESSION_STATUSES}, got {status!r}")
        return FIELDS["session"][status]
    if status is not None:
        raise ValueError(f"{event_type} has no status")
    return FIELDS[event_type]


def event_id(event_type: str, event_ts: str, station_id: str,
             status: str | None = None, session_id: str | None = None) -> str:
    """Deterministic id: the same input row always gives the same id."""
    if event_type == "price_update":
        return f"price_update:{station_id}:{event_ts}"
    if event_type == "session":
        return f"session:{status}:{session_id}"
    return f"meter:{session_id}:{event_ts}"


def _cast(name: str, value: Any) -> Any:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if name in INT_FIELDS:
        return int(value)
    if name in FLOAT_FIELDS:
        return float(value)
    return str(value)


def make_event(event_type: str, event_ts: str, station_id: str,
               status: str | None = None, **fields: Any) -> dict:
    """Build one event dict (without seq; seq is assigned after global sorting)."""
    names = field_names(event_type, status)
    missing = [n for n in names if n not in fields]
    extra = sorted(set(fields) - set(names))
    if missing or extra:
        raise ValueError(f"{event_type}/{status}: missing {missing}, unexpected {extra}")

    event = {
        "event_id": event_id(event_type, str(event_ts), str(station_id), status,
                             fields.get("session_id")),
        "event_type": event_type,
    }
    if status is not None:
        event["status"] = status
    event["event_ts"] = str(event_ts)
    event["station_id"] = str(station_id)
    for name in names:
        event[name] = _cast(name, fields[name])
    return event


def encode(event: dict) -> bytes:
    """Serialise an event for MQTT publish."""
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def decode(payload: bytes) -> dict | None:
    """Parse an MQTT payload back into an event dict.

    Returns None for an empty payload (that is how a retained message is cleared).
    Raises ValueError for malformed messages; the subscriber logs and skips them.
    """
    if not payload:
        return None
    event = json.loads(payload.decode("utf-8"))   # JSONDecodeError is a ValueError
    missing = [k for k in ENVELOPE if k not in event]
    if missing:
        raise ValueError(f"message without {missing}")
    return event
