from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

DEFAULT_ARGS = {
    "owner": "dj",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

PROJECT = "dj-data-pipeline-2026"

@dag(
    dag_id="ingest_orders_to_raw",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["project1", "raw"],
)
def ingest_orders():

    # CHANGE 1: stub → real extract. Pulls one day's slice from the
    # public dataset into a staging table keyed on the logical date.
    @task
    def stage_daily_slice(ds=None, ds_nodash=None):
        hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
        sql = f"""
        CREATE OR REPLACE TABLE `{PROJECT}.raw.orders_stage_{ds_nodash}` AS
        SELECT *
        FROM `bigquery-public-data.thelook_ecommerce.orders`
        WHERE DATE(created_at) = '{ds}'
        """
        hook.insert_job(
            configuration={"query": {"query": sql, "useLegacySql": False}},
            project_id=PROJECT,
            location="US",
        )
        print(f"Staged orders for {ds}")

    # CHANGE 2: load staged data into the date-partitioned raw table.
    @task
    def load_to_raw(ds=None, ds_nodash=None):
        hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
        sql = f"""
        CREATE OR REPLACE TABLE `{PROJECT}.raw.orders`
        PARTITION BY DATE(created_at) AS
        SELECT * FROM `{PROJECT}.raw.orders_stage_{ds_nodash}`
        """
        hook.insert_job(
            configuration={"query": {"query": sql, "useLegacySql": False}},
            project_id=PROJECT,
            location="US",
        )
        print(f"Loaded raw.orders partition for {ds}")

    # CHANGE 3: real quality check — fail loudly if zero rows landed.
    @task
    def quality_check(ds=None):
        hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False, location="US")
        result = hook.get_first(
            f"SELECT COUNT(*) FROM `{PROJECT}.raw.orders` WHERE DATE(created_at) = '{ds}'"
        )
        row_count = result[0]
        if row_count == 0:
            raise ValueError(f"Quality check failed: 0 rows for {ds}")
        print(f"Quality check passed: {row_count} rows for {ds}")

    stage_daily_slice() >> load_to_raw() >> quality_check()

ingest_orders()