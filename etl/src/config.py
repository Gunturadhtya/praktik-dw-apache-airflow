"""Runtime configuration. Everything comes from environment variables (.env)."""
import os


def _db(prefix: str) -> dict:
    return dict(
        host=os.environ[f"{prefix}_HOST"],
        port=int(os.environ.get(f"{prefix}_PORT", "3306")),
        user=os.environ[f"{prefix}_USER"],
        password=os.environ[f"{prefix}_PASSWORD"],
        database=os.environ[f"{prefix}_DB_NAME"],
        charset="utf8mb4",
    )


def oltp_cfg() -> dict:
    return _db("OLTP")


def dw_cfg() -> dict:
    return _db("DW")


def stg_cfg() -> dict:
    """staging-db. Default schema = STG_DB_NAME (etl_control); staging tables are schema-qualified."""
    return _db("STG")


# Max NEW rows read per transactional table per run (a backlog is drained over several runs).
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5000"))
# Timestamp-based watermarks re-read this many minutes back; safe because loads are idempotent.
LOOKBACK_MINUTES = int(os.environ.get("LOOKBACK_MINUTES", "5"))
# Max ids per "WHERE id IN (...)" query.
IN_CHUNK = 500