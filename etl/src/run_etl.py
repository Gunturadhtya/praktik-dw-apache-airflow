"""Local debugging entry point: runs the SAME stage functions as the Airflow DAG, in sequence.

Usage:  python run_etl.py [cron|manual]

Production scheduling, overlap protection (max_active_runs=1) and retries are Airflow's job;
this script has none of that, so do not run it while the DAG is running.
"""
import logging
import sys
import time

import stages

log = logging.getLogger("etl")


def main() -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(message)s")
    trigger = "Manual" if (len(sys.argv) > 1 and sys.argv[1].lower() == "manual") else "Cron"
    meta = stages.init_run(trigger)
    rid, today = meta["run_id"], meta["today"]
    t0 = time.perf_counter()
    try:
        stages.extract_stage(rid, today)
        stages.transform_dims(rid, today)
        stages.transform_facts(rid, today)
        stages.prepare_dim_load(rid, today)
        stages.load_dimensions(rid)
        stages.prepare_fact_load(rid)
        stages.load_facts(rid)
        stages.validate(rid)
        stages.commit_watermarks(rid)
        stages.finish_run(rid, "Success")
        log.info("run %d OK in %.2fs", rid, time.perf_counter() - t0)
        return 0
    except Exception as e:
        log.exception("run %d failed", rid)
        stages.finish_run(rid, "Failed", f"{type(e).__name__}: {e}"[:2000])
        return 1


if __name__ == "__main__":
    sys.exit(main())