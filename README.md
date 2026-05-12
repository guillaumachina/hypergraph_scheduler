# hypergraph_scheduler

Local tooling for extracting Airflow metadata into DuckDB, combining it with DAG hypergraph inputs, and testing schedule changes for reschedulable DS-owned DAGs.

The current focus is still the `recommendation_engine` scope, but the codebase is now structured to support additional DS DAG scopes on other days without changing non-DS upstream DAG schedules or moving DAGs across days.

## What It Does

- exports `dag_run`, `task_instance`, and `task_reschedule` from PostgreSQL
- builds a local DuckDB analytics layer over runtime and waiting-time facts
- loads versioned static dependency inputs for one or more configured DAG scopes
- produces scope-specific reports and schedule proposal artifacts

## Current Workflow

Initialize the local environment once with `uv`:

```bash
uv sync --extra dev
```

1. Export Airflow metadata from PostgreSQL:

   ```bash
   PGPASSWORD=... uv run hypergraph-scheduler export-raw --host <host> --database <database> --user <user>
   ```

2. Load the raw exports into DuckDB:

   ```bash
   uv run hypergraph-scheduler load-raw
   ```

3. Build derived runtime and scoped recommendation_engine views:

   ```bash
   uv run hypergraph-scheduler build-views
   ```

   This step discovers configured scope inputs under `docs/*_inputs/` and builds scope-specific DuckDB views for each one.

4. Generate the candidate report:

   ```bash
   uv run hypergraph-scheduler build-report
   uv run hypergraph-scheduler build-report --scope recommendation_engine
   ```

5. Generate the schedule proposal:

   ```bash
   uv run hypergraph-scheduler build-schedule-proposal
   uv run hypergraph-scheduler build-schedule-proposal --scope recommendation_engine
   ```

Generated data stays local under `data/` and `artifacts/`.

## Repository Layout

```text
hypergraph_scheduler/
├── src/
│   └── hypergraph_scheduler/          Python package for extraction, DuckDB loading, and scheduling logic
├── sql/
│   ├── extract/                       PostgreSQL extraction queries
│   └── transform/                     DuckDB transformation queries and scoped views
├── docs/
│   ├── recommendation_engine_inputs/  One configured DAG scope with graph, model, and scope metadata
│   └── ...                            Workflow notes and project documentation
├── data/
│   ├── raw/                           Local raw exports, ignored by git
│   └── duckdb/                        Local DuckDB files, ignored by git
└── artifacts/                         Generated reports and schedule proposals, ignored by git
```

## Main Commands

- `export-raw`: run the PostgreSQL extracts into `data/raw/`
- `load-raw`: create DuckDB raw tables from local exports
- `build-views`: build runtime summaries and scope-specific optimizer inputs for all configured scopes
- `build-report`: write candidate reports for all scopes, or one scope via `--scope`
- `build-schedule-proposal`: write schedule proposal Markdown and CSV artifacts for all scopes, or one scope via `--scope`
- `init-db`: load raw data and build views in one step

## Setup

1. Install `uv` if it is not already available.
2. Run `uv sync --extra dev` from the repository root to create `.venv` and install the project.
3. Use `uv run hypergraph-scheduler ...` for the workflow commands above.
4. Generated outputs continue to live under `data/raw/`, `data/duckdb/`, and `artifacts/`.

## Scope Model

- only DS-owned DAGs inside a configured scope are treated as reschedulable
- upstream dependencies outside that DS-owned set are modeled as fixed context
- schedule proposals currently optimize start time within the existing scheduled day, not across days
- schedule proposals currently come from a heuristic slot-search, not a full solver

## Configured Scopes

Each configured scope lives under `docs/*_inputs/` and is discovered from a local `scope.json` file.
The existing recommendation_engine scope lives under `docs/recommendation_engine_inputs/` and contains:

- `scope.json`
- `recommendation_engine_dag_dependencies.json`
- `recommendation_engine_schedule_optimization_model.json`
- `dag_schedules_and_dependencies.md`
- `recommendation_engine_schedule_optimization_formulation.md`

This structure allows additional scopes, such as a Monday DS hypergraph, to be added later with their own inputs and artifact prefixes.

These files are committed in this repository so `hypergraph_scheduler` does not depend on a sibling checkout of `recommendation_engine`.
See `docs/recommendation_engine_inputs/README.md` for provenance and refresh instructions.
