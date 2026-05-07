# Local Workflow

## Purpose

This workflow builds a local DuckDB analytics layer from Airflow metadata exports.

Expected raw folders:

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
