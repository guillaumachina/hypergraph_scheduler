# Local Workflow

## Purpose

This workflow builds a local DuckDB analytics layer from Airflow metadata exports.

## Raw Extracts

Run the exporter command, which executes the SQL files in `sql/extract/` with `psql` and saves each result set under `data/raw/`.

```bash
PGPASSWORD=... python -m hypergraph_scheduler export-raw --host <host> --database <database> --user <user>
```

Expected folders:

- `data/raw/dag_run/`
- `data/raw/task_instance/`
- `data/raw/task_reschedule/`

Each folder can contain parquet or csv files.

## Load Into DuckDB

```bash
python -m hypergraph_scheduler load-raw
```

This creates physical raw tables:

- `raw_dag_run`
- `raw_task_instance`
- `raw_task_reschedule`

## Build Derived Views

```bash
python -m hypergraph_scheduler build-views
```

This creates:

- `dag_runs_enriched`
- `task_instances_enriched`
- `sensor_task_runs`
- `sensor_reschedule_summary`
- `sensor_wait_summary`
