"""s08 · Member C · Task 4: DuckDB event storage + analytics queries.

Extends the copied A1 database (config.A2_DB_PATH) with the tables in
sql/schema_a2.sql. Events carry station_id (e.g. KUR-04); data/raw/stations_input.csv
maps it to the A1 key `source_record_id` for joining `ev_chargers`
(stream-table join / enrichment, Week 9 p37-38).

Concurrency note: DuckDB allows only ONE process to open a file read-write.
That is why the subscriber and the dashboard run in the SAME process and share
this store (see s09_dashboard.py). Use one connection guarded by a lock, or
con.cursor() per thread.
"""