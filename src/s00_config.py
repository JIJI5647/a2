"""s00 · Shared · paths and settings for the Assignment 2 pipeline.

All scripts import from here so that paths and MQTT settings live in one place.
Override MQTT settings with environment variables instead of editing code, e.g.

    export A2_TOPIC_ROOT="abcd1234/ass2"
    export A2_BROKER_HOST="localhost"
"""
import os
from pathlib import Path

# --- Paths -------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"            # generator output (stations/prices/sessions/metering CSVs)
EVENTS_DIR = DATA_DIR / "events"      # ordered event stream prepared for publishing
DB_DIR = PROJECT_ROOT / "db"
SQL_DIR = PROJECT_ROOT / "sql"

GENERATOR_SCRIPT = PROJECT_ROOT / "COMP5339_2026s2_Assignment2_data _generator" / "generate_dataset.py"

# Assignment 1 database, copied unchanged into the project (read-only source).
# s02_export_stations.py reads it and copies it into A2_DB_PATH for Task 4.
A1_DB_PATH = DATA_DIR / "a1" / "ev_chargers.duckdb"
A2_DB_PATH = DB_DIR / "ev_chargers_a2.duckdb"

STATIONS_INPUT_CSV = RAW_DIR / "stations_input.csv"   # our A1 chargers in generator format
EVENT_STREAM_PATH = EVENTS_DIR / "event_stream.jsonl"  # one JSON message per line, event-time order

# --- Generator settings --------------------------------------------------------
GEN_START = "2026-10-01"   # first simulated day (must be in the past: real AEMO prices)
GEN_DAYS = 1               # number of simulated days
GEN_SEED = 5339            # fixed seed -> identical data on every run
GEN_METER_EVERY = 5        # minutes between meter readings

# --- MQTT settings (Week 9 lecture p13-17) ------------------------------------
BROKER_HOST = os.getenv("A2_BROKER_HOST", "broker.hivemq.com")
BROKER_PORT = int(os.getenv("A2_BROKER_PORT", "1883"))
# Lecture W9 p15: use a unique topic for Assignment 2, e.g. "<unikey>/ass2".
# TODO: set A2_TOPIC_ROOT to your group's unique prefix before running.
TOPIC_ROOT = os.getenv("A2_TOPIC_ROOT", "UNIKEY/ass2")
QOS = int(os.getenv("A2_QOS", "1"))

# A session is one event type with status start/end (the assignment's
# session_start / session_end events).
EVENT_TYPES = ("price_update", "session", "meter")
SESSION_STATUSES = ("start", "end")

# The broker keeps the last retained message per topic -> latest price per station (W9 p17).
RETAIN_TYPES = ("price_update",)

MIN_PUBLISH_DELAY_S = 0.1   # assignment requirement: at least 0.1 s between messages

# Default number of events s05 publishes (for development); use --all for the full stream.
DEV_MAX_EVENTS = 500


# Topics: <root>/price/<station_id> (retained) and <root>/charging/<station_id>.
# MQTT keeps order only within a topic, so a station's session and meter events
# share one topic and always arrive in order.
TOPIC_GROUPS = {"price_update": "price", "session": "charging", "meter": "charging"}


def topic_for(event_type: str, station_id: str) -> str:
    """e.g. UNIKEY/ass2/price/KUR-04, UNIKEY/ass2/charging/KUR-04."""
    return f"{TOPIC_ROOT}/{TOPIC_GROUPS[event_type]}/{station_id}"
