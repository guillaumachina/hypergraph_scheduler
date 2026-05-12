# Local Workflow

## Purpose

This workflow builds a local DuckDB analytics layer from Airflow metadata exports and then materializes scope-specific views for each configured DAG hypergraph.

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

This step also discovers each configured scope under `docs/*_inputs/`, loads its versioned static inputs, and creates scope-specific views in DuckDB.

This creates:

- `dag_runs_enriched`
- `task_instances_enriched`
- `sensor_task_runs`
- `sensor_reschedule_summary`
- `sensor_wait_summary`

## Generate Candidate Report

```bash
uv run hypergraph-scheduler build-report
uv run hypergraph-scheduler build-report --scope recommendation_engine
```

Without `--scope`, this writes one candidate report per configured scope.
With `--scope`, it writes only that scope's report.

Example output:

- `artifacts/recommendation_engine_candidate_report.md`

## Generate Schedule Proposal

```bash
uv run hypergraph-scheduler build-schedule-proposal
uv run hypergraph-scheduler build-schedule-proposal --scope recommendation_engine
```

Without `--scope`, this writes one proposal per configured scope.
With `--scope`, it writes only that scope's proposal artifacts.

Example output:

- `artifacts/recommendation_engine_schedule_proposal.md`
- `artifacts/recommendation_engine_schedule_proposal.csv`

## Configured Scope Inputs

Each scope is discovered from `docs/*_inputs/scope.json` and uses local committed copies of its dependency and optimization inputs.

The current recommendation_engine scope uses:

- `docs/recommendation_engine_inputs/scope.json`
- `docs/recommendation_engine_inputs/recommendation_engine_dag_dependencies.json`
- `docs/recommendation_engine_inputs/recommendation_engine_schedule_optimization_model.json`

Supporting reference material is also versioned alongside them:

- `docs/recommendation_engine_inputs/dag_schedules_and_dependencies.md`
- `docs/recommendation_engine_inputs/recommendation_engine_schedule_optimization_formulation.md`

Those files are the source of the graph nodes, graph edges, seed-edge sensor mapping, and optimization defaults loaded into DuckDB by `build-views`.
