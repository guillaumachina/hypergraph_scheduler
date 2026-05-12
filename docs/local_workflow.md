# Local Workflow

## Purpose

This workflow builds a local DuckDB analytics layer from Airflow metadata exports.

## Environment Setup

Create and populate the local virtual environment with `uv`:

```bash
uv sync --extra dev
```

Run all workflow commands through `uv run` so they use the project environment from `.venv`.

## Raw Extracts

Run the exporter command, which executes the SQL files in `sql/extract/` with `psql` and saves each result set under `data/raw/`.

```bash
PGPASSWORD=... uv run hypergraph-scheduler export-raw --host <host> --database <database> --user <user>
```

Expected folders:

- `data/raw/dag_run/`
- `data/raw/task_instance/`
- `data/raw/task_reschedule/`

Each folder can contain parquet or csv files.

## Load Into DuckDB

```bash
uv run hypergraph-scheduler load-raw
```

This creates physical raw tables:

- `raw_dag_run`
- `raw_task_instance`
- `raw_task_reschedule`

## Build Derived Views

```bash
uv run hypergraph-scheduler build-views
```

This step also loads the versioned recommendation_engine static inputs stored under `docs/recommendation_engine_inputs/`.

This creates:

- `dag_runs_enriched`
- `task_instances_enriched`
- `sensor_task_runs`
- `sensor_reschedule_summary`
- `sensor_wait_summary`

## Generate Candidate Report

```bash
uv run hypergraph-scheduler build-report
```

This writes:

- `artifacts/recommendation_engine_candidate_report.md`

## Generate Schedule Proposal

```bash
uv run hypergraph-scheduler build-schedule-proposal
```

This writes:

- `artifacts/recommendation_engine_schedule_proposal.md`
- `artifacts/recommendation_engine_schedule_proposal.csv`

## Versioned Recommendation Engine Inputs

The recommendation_engine scoped analysis uses local committed copies of the dependency and optimization inputs:

- `docs/recommendation_engine_inputs/recommendation_engine_dag_dependencies.json`
- `docs/recommendation_engine_inputs/recommendation_engine_schedule_optimization_model.json`

Supporting reference material is also versioned alongside them:

- `docs/recommendation_engine_inputs/dag_schedules_and_dependencies.md`
- `docs/recommendation_engine_inputs/recommendation_engine_schedule_optimization_formulation.md`

Those files are the source of the graph nodes, graph edges, and optimization defaults loaded into DuckDB by `build-views`.
