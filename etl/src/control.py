"""Permanent control store (MySQL schema `etl_control` on staging-db): run log, metrics, rejects,
snapshot manifest, validation results and the incremental-extraction state (watermarks).

Replaces the old SQLite file. Same public methods, plus:
  * task-aware metrics (a retried Airflow task deletes its own metrics first),
  * PENDING watermarks: extract writes them to *_pending, `promote_pending` moves them into the real
    state ONLY after validation succeeded (same rule as before: never advance before the DW commit).
"""
import pymysql

from config import stg_cfg

DDL = [
    """CREATE TABLE IF NOT EXISTS etl_run_log (
         run_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
         trigger_type  ENUM('Manual','Cron') NOT NULL,
         started_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
         finished_at   DATETIME NULL,
         status        ENUM('Running','Success','Failed') NOT NULL DEFAULT 'Running',
         error_message TEXT NULL)""",
    """CREATE TABLE IF NOT EXISTS etl_table_metrics (
         metric_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
         run_id         BIGINT NOT NULL,
         task_name      VARCHAR(50) NULL,
         stage          ENUM('Extract','Transform','Load') NOT NULL,
         table_name     VARCHAR(100) NOT NULL,
         rows_in        INT NOT NULL DEFAULT 0,
         rows_out       INT NOT NULL DEFAULT 0,
         rows_rejected  INT NOT NULL DEFAULT 0,
         duration_sec   DOUBLE NOT NULL DEFAULT 0,
         throughput_rps DOUBLE NULL,
         logged_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
         KEY ix_run_task (run_id, task_name))""",
    """CREATE TABLE IF NOT EXISTS rejected_rows (
         reject_id    BIGINT AUTO_INCREMENT PRIMARY KEY,
         run_id       BIGINT NOT NULL,
         source_table VARCHAR(100) NOT NULL,
         source_id    VARCHAR(100) NULL,
         rule_name    VARCHAR(100) NOT NULL,
         reason       TEXT NULL,
         rejected_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
         KEY ix_run (run_id))""",
    """CREATE TABLE IF NOT EXISTS transform_test_result (
         test_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
         run_id         BIGINT NULL,
         rule_name      VARCHAR(200) NOT NULL,
         before_value   VARCHAR(100) NULL,
         expected_value VARCHAR(100) NULL,
         actual_value   VARCHAR(100) NULL,
         status         ENUM('PASS','FAIL') NOT NULL,
         tested_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
         KEY ix_run (run_id))""",
    """CREATE TABLE IF NOT EXISTS etl_snapshot_manifest (
         run_id       BIGINT NOT NULL,
         source_table VARCHAR(100) NOT NULL,
         pending_rows BIGINT NOT NULL,
         max_id       BIGINT NULL,
         captured_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
         PRIMARY KEY (run_id, source_table))""",
    "CREATE TABLE IF NOT EXISTS etl_state (k VARCHAR(100) PRIMARY KEY, v VARCHAR(255) NOT NULL)",
    "CREATE TABLE IF NOT EXISTS etl_product_fp (product_id BIGINT PRIMARY KEY, fp VARCHAR(255) NOT NULL)",
    """CREATE TABLE IF NOT EXISTS etl_state_pending (
         run_id BIGINT NOT NULL, k VARCHAR(100) NOT NULL, v VARCHAR(255) NOT NULL,
         PRIMARY KEY (run_id, k))""",
    """CREATE TABLE IF NOT EXISTS etl_product_fp_pending (
         run_id BIGINT NOT NULL, product_id BIGINT NOT NULL, fp VARCHAR(255) NOT NULL,
         PRIMARY KEY (run_id, product_id))""",
]


class Control:
    def __init__(self, ensure: bool = False):
        self.c = pymysql.connect(**stg_cfg(), autocommit=True)   # tuple cursors
        self.task = None                                         # set by stages.run_ctx for metrics
        if ensure:
            with self.c.cursor() as cur:
                for stmt in DDL:
                    cur.execute(stmt)

    def _exec(self, sql, params=()):
        with self.c.cursor() as cur:
            cur.execute(sql, params)

    def _many(self, sql, rows):
        if rows:
            with self.c.cursor() as cur:
                cur.executemany(sql, rows)

    # ---- run log -------------------------------------------------------
    def fail_stale_runs(self):
        """Called by the init task (max_active_runs=1): any 'Running' row is a crashed earlier run."""
        self._exec("UPDATE etl_run_log SET status='Failed', finished_at=NOW(),"
                   " error_message='run did not finish (task killed or container stopped)'"
                   " WHERE status='Running'")

    def start_run(self, trigger: str) -> int:
        with self.c.cursor() as cur:
            cur.execute("INSERT INTO etl_run_log (trigger_type) VALUES (%s)", (trigger,))
            return cur.lastrowid

    def finish_run(self, run_id: int, status: str, error):
        self._exec("UPDATE etl_run_log SET finished_at=NOW(), status=%s, error_message=%s WHERE run_id=%s",
                   (status, error, run_id))

    # ---- metrics / rejects / manifest / tests --------------------------
    def delete_task_metrics(self, run_id, task):
        self._exec("DELETE FROM etl_table_metrics WHERE run_id=%s AND task_name=%s", (run_id, task))

    def add_metric(self, run_id, stage, table, rows_in, rows_out, rejected, seconds, throughput):
        self._exec(
            "INSERT INTO etl_table_metrics (run_id,task_name,stage,table_name,rows_in,rows_out,rows_rejected,"
            "duration_sec,throughput_rps) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (run_id, self.task, stage, table, rows_in, rows_out, rejected, seconds, throughput))

    def copy_rejects(self, run_id):
        """stg_transform.rejected_rows (transform + load-prep rejects) -> permanent rejected_rows."""
        self._exec("DELETE FROM rejected_rows WHERE run_id=%s", (run_id,))
        self._exec("INSERT INTO rejected_rows (run_id,source_table,source_id,rule_name,reason) "
                   "SELECT batch_id,source_table,source_id,rule_name,reason "
                   "FROM stg_transform.rejected_rows WHERE batch_id=%s", (run_id,))

    def add_manifest(self, run_id, rows):
        self._many("REPLACE INTO etl_snapshot_manifest (run_id,source_table,pending_rows,max_id)"
                   " VALUES (%s,%s,%s,%s)",
                   [(run_id, t, int(n or 0), int(mx or 0)) for t, n, mx in rows])  # MySQL returns Decimal

    def delete_tests(self, run_id):
        self._exec("DELETE FROM transform_test_result WHERE run_id=%s", (run_id,))

    def add_test(self, run_id, rule, before, expected, actual, ok: bool):
        self._exec("INSERT INTO transform_test_result (run_id,rule_name,before_value,expected_value,"
                   "actual_value,status) VALUES (%s,%s,%s,%s,%s,%s)",
                   (run_id, rule, str(before), str(expected), str(actual), "PASS" if ok else "FAIL"))

    # ---- incremental state ---------------------------------------------
    def load_state(self) -> dict:
        with self.c.cursor() as cur:
            cur.execute("SELECT k, v FROM etl_state")
            return {k: v for k, v in cur.fetchall()}

    def load_product_fp(self) -> dict:
        with self.c.cursor() as cur:
            cur.execute("SELECT product_id, fp FROM etl_product_fp")
            return {pid: fp for pid, fp in cur.fetchall()}

    def save_pending(self, run_id, state_updates: dict, fp_updates: dict):
        """Extract output: new watermarks, parked until the run is validated."""
        self.clear_pending(run_id)
        self._many("INSERT INTO etl_state_pending (run_id,k,v) VALUES (%s,%s,%s)",
                   [(run_id, k, str(v)) for k, v in state_updates.items()])
        self._many("INSERT INTO etl_product_fp_pending (run_id,product_id,fp) VALUES (%s,%s,%s)",
                   [(run_id, pid, fp) for pid, fp in fp_updates.items()])

    def clear_pending(self, run_id):
        self._exec("DELETE FROM etl_state_pending WHERE run_id=%s", (run_id,))
        self._exec("DELETE FROM etl_product_fp_pending WHERE run_id=%s", (run_id,))

    def promote_pending(self, run_id):
        """Advance watermarks. Called ONLY after DW load + validation succeeded. Idempotent."""
        self.c.begin()
        try:
            with self.c.cursor() as cur:
                cur.execute("REPLACE INTO etl_state (k,v) SELECT k,v FROM etl_state_pending WHERE run_id=%s",
                            (run_id,))
                cur.execute("REPLACE INTO etl_product_fp (product_id,fp) "
                            "SELECT product_id,fp FROM etl_product_fp_pending WHERE run_id=%s", (run_id,))
                cur.execute("DELETE FROM etl_state_pending WHERE run_id=%s", (run_id,))
                cur.execute("DELETE FROM etl_product_fp_pending WHERE run_id=%s", (run_id,))
            self.c.commit()
        except Exception:
            self.c.rollback()
            raise

    def purge_pending(self, cutoff_run_id):
        self._exec("DELETE FROM etl_state_pending WHERE run_id<=%s", (cutoff_run_id,))
        self._exec("DELETE FROM etl_product_fp_pending WHERE run_id<=%s", (cutoff_run_id,))

    def close(self):
        self.c.close()