# hypergraph_scheduler

Local tooling for extracting Airflow metadata into DuckDB, combining it with dependency-graph inputs, and testing schedule changes for reschedulable DAGs.

The current focus is the `recommendation_engine` scope: DS-owned DAGs that can be moved, plus their fixed upstream context.

## What It Does

- exports `dag_run`, `task_instance`, and `task_reschedule` from PostgreSQL
- builds a local DuckDB analytics layer over runtime and waiting-time facts
- loads static dependency inputs for the scoped DAG graph
- produces recommendation_engine reports and schedule proposal artifacts

## Current Workflow

1. Export Airflow metadata from PostgreSQL:

   ```bash
   PGPASSWORD=... python -m hypergraph_scheduler export-raw --host <host> --database <database> --user <user>
   ```

2. Load the raw exports into DuckDB:

   ```bash
   python -m hypergraph_scheduler load-raw
   ```

3. Build derived runtime and scoped recommendation_engine views:

   ```bash
   python -m hypergraph_scheduler build-views
   ```

4. Generate the candidate report:

   ```bash
   python -m hypergraph_scheduler build-report
   ```

5. Generate the schedule proposal:

   ```bash
   python -m hypergraph_scheduler build-schedule-proposal
   ```

Generated data stays local under `data/` and `artifacts/`.

## Repository Layout

- `src/hypergraph_scheduler/`: Python package for DuckDB pipelines and scheduling logic
- `sql/extract/`: source SQL used against PostgreSQL metadata tables
- `sql/transform/`: DuckDB transformation SQL
- `docs/`: project notes and scoped workflow documentation
- `data/raw/`: local exported data files, ignored by git
- `data/duckdb/`: local DuckDB databases, ignored by git
- `artifacts/`: generated outputs, ignored by git

## Main Commands

- `export-raw`: run the PostgreSQL extracts into `data/raw/`
- `load-raw`: create DuckDB raw tables from local exports
- `build-views`: build runtime summaries and scoped optimizer inputs
- `build-report`: write `artifacts/recommendation_engine_candidate_report.md`
- `build-schedule-proposal`: write schedule proposal Markdown and CSV artifacts
- `init-db`: load raw data and build views in one step

## Setup

1. Create a Python virtual environment.
2. Install the project in editable mode with `pip install -e .`.
3. Use the commands above to populate `data/raw/`, `data/duckdb/`, and `artifacts/`.

## Current Scope

- only DS-owned `recommendation_engine` DAGs are treated as reschedulable
- upstream dependencies are modeled as fixed context
- schedule proposals currently come from a heuristic slot-search, not a full solver
