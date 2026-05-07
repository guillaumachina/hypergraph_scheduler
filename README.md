# hypergraph_scheduler

Local project for extracting Airflow metadata into DuckDB, modeling DAG dependencies, and testing scheduling algorithms for DAG rescheduling.

## Scope

- SQL extraction from the Airflow metadata database
- DuckDB staging and derived analytics tables
- Optimization and heuristic scheduling experiments
- Documentation for the dependency graph, runtime facts, and optimization approach

## Layout

- `src/hypergraph_scheduler/`: Python package for DuckDB pipelines and scheduling logic
- `sql/extract/`: source SQL used against PostgreSQL metadata tables
- `sql/transform/`: DuckDB transformation SQL
- `docs/`: project notes, formulation, and experiment docs
- `data/raw/`: local exported data files, ignored by git
- `data/duckdb/`: local DuckDB databases, ignored by git
- `artifacts/`: generated outputs, ignored by git

## Quick Start

1. Create a Python virtual environment.
2. Install the project in editable mode with `pip install -e .`.
3. Put raw exports under `data/raw/`.
4. Build local DuckDB tables from those exports.

## First Workflow

1. Export the PostgreSQL source data with:

   ```bash
   PGPASSWORD=... python -m hypergraph_scheduler export-raw --host <host> --database <database> --user <user>
   ```

2. The command writes csv extracts under `data/raw/`, for example:

   - `data/raw/dag_run/`
   - `data/raw/task_instance/`
   - `data/raw/task_reschedule/`

3. Load raw files into DuckDB with:

   ```bash
   python -m hypergraph_scheduler load-raw
   ```

4. Build derived views with:

   ```bash
   python -m hypergraph_scheduler build-views
   ```

5. Query the local database at `data/duckdb/hypergraph_scheduler.duckdb`.
