"""s07 · Member B · Task 3: in-memory current state for the dashboard.

Stateful stream processing (Week 9 p25-26): the subscriber folds each event into
this state; the dashboard reads a snapshot on every refresh. DuckDB keeps the full
history (s08_event_store.py); this object only holds what the map needs *now*.

Thread safety: MQTT callbacks run in paho's network thread while Dash callbacks
run in the web server's threads, so every access goes through self._lock.
"""
from copy import deepcopy
from datetime import datetime
from threading import RLock


class LiveState:
    def __init__(self):
        self._lock = RLock()
        self._stations = {}
        self._ended_sessions = set()

    def apply(self, event):
        station_id = event["station_id"]
        event_type = event["event_type"]

        with self._lock:
            station = self._stations.setdefault(
                station_id,
                {
                    "station_id": station_id,
                    "price_per_kwh": None,
                    "price_ts": None,
                    "active_sessions": {},
                },
            )

            if event_type == "price_update":
                incoming_time = datetime.fromisoformat(
                    event["event_ts"]
                )

                previous_time = (
                    datetime.fromisoformat(station["price_ts"])
                    if station["price_ts"] is not None
                    else None
                )

                if previous_time is None or incoming_time > previous_time:
                    station["price_per_kwh"] = event["price_per_kwh"]
                    station["price_ts"] = event["event_ts"]
            elif event_type == "session" and event["status"] == "start":
                session_id = event["session_id"]
                session_key = (station_id, session_id)

                if session_key in self._ended_sessions:
                    return

                sessions = station["active_sessions"]

                sessions.setdefault(
                    session_id,
                    {
                        "session_id": session_id,
                        "connector_id": event["connector_id"],
                        "vehicle_model": event["vehicle_model"],
                        "start_ts": event["event_ts"],
                        "latest_meter": None,
                        "meter_readings": {},
                    },
                )
            elif event_type == "session" and event["status"] == "end":
                session_id = event["session_id"]
                session_key = (station_id, session_id)

                station["active_sessions"].pop(session_id, None)
                self._ended_sessions.add(session_key)
            elif event_type == "meter":
                session_id = event["session_id"]
                session = station["active_sessions"].get(session_id)

                if session is None:
                    return

                reading = deepcopy(event)

                incoming_key = (
                    datetime.fromisoformat(reading["event_ts"]),
                    reading["seq"],
                )

                previous = session["latest_meter"]

                previous_key = (
                    (
                        datetime.fromisoformat(previous["event_ts"]),
                        previous["seq"],
                    )
                    if previous is not None
                    else None
                )

                session["meter_readings"][reading["event_id"]] = reading

                if previous_key is None or incoming_key > previous_key:
                    session["latest_meter"] = reading
                
    def snapshot(self):
        with self._lock:
            result = deepcopy(self._stations)
        for station in result.values():
            station["occupied_connectors"] = len({
                session["connector_id"]
                for session in station["active_sessions"].values()
            })
        return result

    def session_curve(self, station_id, session_id=None):
        with self._lock:
            station = self._stations.get(station_id)

            if station is None:
                return []

            sessions = station["active_sessions"]

            if not sessions:
                return []

            if session_id is None:
                session_id = min(sessions)

            session = sessions.get(session_id)

            if session is None:
                return []

            readings = deepcopy(
                list(session["meter_readings"].values())
            )

        return sorted(
            readings,
            key=lambda reading: (
                datetime.fromisoformat(reading["event_ts"]),
                reading["seq"],
            ),
        )
