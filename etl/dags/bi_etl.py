"""bi_etl: OLTP -> stg_extract -> stg_transform -> stg_load -> DW, with validation and watermark commit.

  init -> extract -> [transform_dims, transform_facts] -> prepare_dim_load -> load_dimensions
       -> prepare_fact_load -> load_facts -> validate -> commit_watermarks -> mark_success
  any task failed -> mark_failed (records the failure, then fails so the DAG run is red)

Only tiny metadata (run_id, today) goes through XCom; the data travels through the staging layers.
`stages` is imported INSIDE the tasks so DAG parsing stays fast and needs no DB connection.
"""
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

DEFAULTS = dict(retries=2, retry_delay=timedelta(minutes=1))


@dag(
    dag_id="bi_etl",
    schedule="*/5 * * * *",          # Airflow is not built for every-minute runs; 5 min is a sane start
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,               # replaces the old flock overlap protection
    default_args=DEFAULTS,
    tags=["bi", "etl"],
)
def bi_etl():

    @task
    def init(dag_run=None):
        import stages
        return stages.init_run(trigger=getattr(dag_run, "run_type", "scheduled"))

    @task
    def extract(meta):
        import stages
        stages.extract_stage(meta["run_id"], meta["today"])

    @task
    def transform_dims(meta):
        import stages
        stages.transform_dims(meta["run_id"], meta["today"])

    @task
    def transform_facts(meta):
        import stages
        stages.transform_facts(meta["run_id"], meta["today"])

    @task
    def prepare_dim_load(meta):
        import stages
        stages.prepare_dim_load(meta["run_id"], meta["today"])

    @task
    def load_dimensions(meta):
        import stages
        stages.load_dimensions(meta["run_id"])

    @task
    def prepare_fact_load(meta):
        import stages
        stages.prepare_fact_load(meta["run_id"])

    @task
    def load_facts(meta):
        import stages
        stages.load_facts(meta["run_id"])

    @task
    def validate(meta):
        import stages
        stages.validate(meta["run_id"])

    @task
    def commit_watermarks(meta):
        import stages
        stages.commit_watermarks(meta["run_id"])

    @task
    def mark_success(meta):
        import stages
        stages.finish_run(meta["run_id"], "Success")

    @task(trigger_rule="one_failed", retries=0)
    def mark_failed(meta):
        import stages
        stages.finish_run(meta["run_id"], "Failed", "one or more tasks failed - see the Airflow task logs")
        raise RuntimeError("ETL run failed")   # keep the DAG run red (mark_failed is a leaf task)

    m = init()
    e = extract(m)
    td, tf = transform_dims(m), transform_facts(m)
    pdl, ld = prepare_dim_load(m), load_dimensions(m)
    pfl, lf = prepare_fact_load(m), load_facts(m)
    v, w = validate(m), commit_watermarks(m)
    ok, bad = mark_success(m), mark_failed(m)

    e >> [td, tf] >> pdl >> ld >> pfl >> lf >> v >> w >> ok
    [m, e, td, tf, pdl, ld, pfl, lf, v, w] >> bad


bi_etl()