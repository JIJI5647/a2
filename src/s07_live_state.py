"""s07 · Member B · Task 3: in-memory current state for the dashboard.

Stateful stream processing (Week 9 p25-26): the subscriber folds each event into
this state; the dashboard reads a snapshot on every refresh. DuckDB keeps the full
history (s08_event_store.py); this object only holds what the map needs *now*.

Thread safety: MQTT callbacks run in paho's network thread while Dash callbacks
run in the web server's threads, so every access goes through self._lock.
"""