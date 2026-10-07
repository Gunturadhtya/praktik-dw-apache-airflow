"""One-off cutover helper: copy watermarks + product fingerprints from the old SQLite control.db
into etl_control on staging-db, so the first Airflow run does NOT re-read the whole source.

Usage (inside the airflow container, with the old file copied in):
    python migrate_sqlite_state.py /path/to/control.db
"""
import sqlite3
import sys

from control import Control


def main(path: str) -> None:
    lite = sqlite3.connect(path)
    state = lite.execute("SELECT key, value FROM etl_state").fetchall()
    fps = lite.execute("SELECT product_id, fp FROM etl_product_fp").fetchall()
    lite.close()

    ctl = Control(ensure=True)
    try:
        with ctl.c.cursor() as cur:
            if state:
                cur.executemany("REPLACE INTO etl_state (k,v) VALUES (%s,%s)", state)
            if fps:
                cur.executemany("REPLACE INTO etl_product_fp (product_id,fp) VALUES (%s,%s)", fps)
    finally:
        ctl.close()
    print(f"migrated {len(state)} state keys and {len(fps)} product fingerprints")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])