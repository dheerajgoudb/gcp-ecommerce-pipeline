from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

DEFAULT_ARGS = {
    "owner": "dj",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="ingest_orders_to_raw",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["project1", "raw"],
)
def ingest_orders():

    @task
    def land_source_file(ds=None):
        """Day 3 scope: pull source CSV for {ds} into GCS landing.
        Tonight: stub that logs the partition date."""
        print(f"Would land orders file for {ds}")
        return f"orders/{ds}/orders.csv"

    load_to_raw = GCSToBigQueryOperator(
        task_id="load_to_raw",
        bucket="dj-pipeline-landing",
        source_objects=["orders/{{ ds }}/orders.csv"],
        destination_project_dataset_table="dj-data-pipeline-2026.raw.orders${{ ds_nodash }}",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    @task
    def quality_check():
        """Day 4 scope: row count + null checks on raw.orders."""
        print("Quality checks placeholder")

    land_source_file() >> load_to_raw >> quality_check()

ingest_orders()