# E-commerce Analytics Pipeline on GCP

End-to-end ELT pipeline: raw events → GCS → BigQuery → dbt-modeled
star schema → dashboard. Orchestrated with Airflow, tested in CI.

## Architecture
[diagram placeholder — draw.io or Excalidraw]

GCS (landing) → Airflow DAG → BQ `raw` → dbt → `staging` → `marts` → Looker Studio

## Stack
Airflow 2.x (Docker) · BigQuery · dbt-core · GCS · GitHub Actions · Python 3.11

## Dataset
[TheLook eCommerce — BigQuery public dataset]

## Pipeline design
- **raw**: ingested as-is, partitioned by load date
- **staging**: typed, deduped, tested (dbt)
- **marts**: star schema — fct_orders, dim_customers, dim_products
- Idempotent loads · backfill-safe · data quality tests in CI

## Roadmap
- [ ] DAG 1: GCS → BQ raw load
- [ ] dbt staging models + tests
- [ ] Star schema marts
- [ ] CI: lint + dbt test on PR
- [ ] Dashboard + cost notes

