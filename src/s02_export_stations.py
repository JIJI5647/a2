"""s02 · Member A · Task 1 prep: export our A1 DC chargers into the generator's station CSV format.

Input : Assignment 1 DuckDB (config.A1_DB_PATH), view `dc_chargers` (432 rows).
Output: data/raw/stations_input.csv with columns
        station_id, name, lat, lon, site_type, connectors, max_kw, source_record_id
        and a copy of the A1 database at config.A2_DB_PATH (Task 4).

Usage:
    python src/s02_export_stations.py
"""
import argparse
import shutil
import duckdb
import s00_config as config

GENERATOR_COLUMNS = ["station_id", "name", "lat", "lon", "site_type", "connectors", "max_kw"]


def copy_a1_database() -> None:
    """Copy (never modify) the A1 database so Task 4 extends a separate file."""
    config.DB_DIR.mkdir(parents=True, exist_ok=True)
    if config.A2_DB_PATH.exists():
        print(f"keep existing {config.A2_DB_PATH}")
        return
    shutil.copy2(config.A1_DB_PATH, config.A2_DB_PATH)
    print(f"copied A1 db -> {config.A2_DB_PATH}")


def load_dc_chargers(con: duckdb.DuckDBPyConnection):
    """Return the DC chargers as a DataFrame in generator format.

    Mapping rules (document in the report):
      - station_id: demo-style code = first 3 letters of the LGA name in upper case
        + per-code running number, e.g. RAN-01. 'UNK' when LGANAME is NULL.
        Plain 3-letter codes collide (e.g. NOR = Northern Beaches / North Sydney),
        the running number makes the id unique.
      - source_record_id: A1 primary key, kept as an extra column so events can be
        joined back to ev_chargers (the generator ignores extra columns).
      - name: Station_name is NULL for 431/432 DC chargers -> "Operator - LGA".
      - site_type: 'urban' if the SA4 lies in GCC 'Greater Sydney', else 'regional'.
      - connectors: Number_of_plugs as is (no cap).
      - max_kw: Charger_rating '50 kW' -> 50.0; for mixed ratings such as
        '2x350kW & 2x175kW' the highest value (350). No NULLs among DC chargers.
    """
    # The A1 db stores GEOMETRY columns; the spatial extension is needed to bind them.
    con.execute("INSTALL spatial; LOAD spatial;")
    return con.sql("""
        WITH base AS (
            SELECT
                d.source_record_id,
                coalesce(upper(left(regexp_replace(d.LGANAME, '[^A-Za-z]', '', 'g'), 3)), 'UNK') AS code,
                coalesce(d.Station_name,
                         d.Operator || ' - ' || coalesce(d.LGANAME, d.SA4_NAME26)) AS name,
                d.Latitude  AS lat,
                d.Longitude AS lon,
                CASE WHEN s.GCC_NAME26 = 'Greater Sydney' THEN 'urban' ELSE 'regional' END AS site_type,
                d.Number_of_plugs AS connectors,
                -- '2x350kW & 2x175kW' -> 350: take the highest "<number> kW" value
                list_max(CAST(
                    regexp_extract_all(d.Charger_rating, '([0-9.]+)\\s*kW', 1) AS DOUBLE[])) AS max_kw,
                d.LGANAME
            FROM dc_chargers d
            JOIN sa4_regions s ON s.SA4_CODE26 = d.SA4_CODE26
        )
        SELECT
            code || '-' || lpad(CAST(row_number() OVER (
                PARTITION BY code ORDER BY LGANAME, source_record_id) AS VARCHAR), 2, '0') AS station_id,
            name, lat, lon, site_type, connectors, max_kw, source_record_id
        FROM base
        ORDER BY station_id
    """).df()


def main() -> None:
    ap = argparse.ArgumentParser()
    args = ap.parse_args()

    copy_a1_database()
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(config.A1_DB_PATH), read_only=True)
    df = load_dc_chargers(con)
    con.close()

    assert df["station_id"].is_unique, "duplicate station_id"
    nulls = df[GENERATOR_COLUMNS].isna().sum()
    assert not nulls.any(), f"NULLs in generator columns:\n{nulls[nulls > 0]}"
    df[GENERATOR_COLUMNS + ["source_record_id"]].to_csv(config.STATIONS_INPUT_CSV, index=False)
    print(f"wrote {len(df)} stations -> {config.STATIONS_INPUT_CSV}")


if __name__ == "__main__":
    main()
