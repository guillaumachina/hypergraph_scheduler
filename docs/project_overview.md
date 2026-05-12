# Project Overview

This project is the working area for building a local analytics and optimization workflow around Airflow DAG rescheduling across multiple DS-owned DAG scopes.

## Planned Workflow

1. Extract relevant metadata tables from PostgreSQL.
2. Load raw data into DuckDB.
3. Build cleaned runtime and waiting-time facts.
4. Join runtime facts with the static dependency graph.
5. Test optimization and heuristic scheduling strategies.

## Current Focus

The first optimization slice is still recommendation_engine, but the codebase is now structured to support additional DS DAG hypergraphs on other scheduled days.

- each configured scope defines its own DS-owned seed DAGs as candidates for schedule changes
- recursively required upstream DAGs are treated as fixed context for dependency and waiting analysis
- additional DS DAG families can be added later as new controllable scopes by adding another `docs/*_inputs/` directory

Current scoped outputs:

- scope-specific runtime summary views in DuckDB
- edge-level wait estimates for mapped seed dependencies within each scope
- generated Markdown reports under `artifacts/` for each configured scope
- heuristic schedule proposal artifacts for each configured scope
- versioned scope inputs under `docs/*_inputs/`

## Initial Deliverables

- reusable extraction SQL
- repeatable DuckDB load pipeline
- documented optimizer input tables
- baseline scheduling algorithms

## Input Provenance

Each scope's dependency graph and optimization defaults are committed with this repository under `docs/*_inputs/`.
The current recommendation_engine scope was derived from the recommendation_engine DAG definitions and recursively referenced upstream DAGs across the related repos.
The vendored copies make the scheduler reproducible without requiring parallel checkouts of the source DAG repositories.
