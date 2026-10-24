"""s03 · Member A · Task 1: run the provided generator on our A1 station list.

Wraps COMP5339_2026s2_Assignment2_data _generator/generate_dataset.py (unchanged)
so the exact arguments are recorded in code and reproducible.

Output (data/raw/): stations.csv, prices_hourly.csv, sessions.csv,
                    session_metering.csv, aemo_cache/

Usage:
    python src/s03_generate_dataset.py
"""
import subprocess
import sys

import s00_config as config


def main() -> None:
    if not config.STATIONS_INPUT_CSV.exists():
        sys.exit("run src/s02_export_stations.py first")

    cmd = [
        sys.executable, str(config.GENERATOR_SCRIPT),
        "--stations", str(config.STATIONS_INPUT_CSV),
        "--start", config.GEN_START,
        "--days", str(config.GEN_DAYS),
        "--seed", str(config.GEN_SEED),
        "--out", str(config.RAW_DIR),
        # fewer meter readings -> shorter stream (meter events are ~60% of all messages)
        "--meter-every", str(config.GEN_METER_EVERY),
    ]
    print("running:", " ".join(cmd))
    subprocess.run(cmd, check=True)   # the generator prints its own summary


if __name__ == "__main__":
    main()
