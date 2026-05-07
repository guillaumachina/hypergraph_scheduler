# Project Overview

This project is the working area for building a local analytics and optimization workflow around Airflow DAG rescheduling.

## Planned Workflow

1. Extract relevant metadata tables from PostgreSQL.
2. Load raw data into DuckDB.
3. Build cleaned runtime and waiting-time facts.
4. Join runtime facts with the static dependency graph.
5. Test optimization and heuristic scheduling strategies.

## Current Focus

The first optimization slice is restricted to recommendation_engine DAGs that the DS team can actually reschedule.

- recommendation_engine seed DAGs are treated as candidate DAGs for schedule changes
- recursively required upstream DAGs are treated as fixed context for dependency and waiting analysis
- additional non-recommendation-engine DAG families can be added later as new controllable scopes

Current scoped outputs:

- recommendation_engine runtime summary views in DuckDB
- edge-level wait estimates for mapped recommendation_engine seed dependencies
- generated Markdown report under `artifacts/` for the five reschedulable seed DAGs
- heuristic schedule proposal artifacts for the five reschedulable seed DAGs

## Initial Deliverables

- reusable extraction SQL
- repeatable DuckDB load pipeline
- documented optimizer input tables
- baseline scheduling algorithms# Project Overview

This project is the working area for building a local analytics and optimization workflow around Airflow DAG rescheduling.

## Planned Workflow

1. Extract relevant metadata tables from PostgreSQL.
2. Load raw data into DuckDB.
3. Build cleaned runtime and waiting-time facts.
4. Join runtime facts with the static dependency graph.
5. Test optimization and heuristic scheduling strategies.

## Current Focus

The first optimization slice is restricted to recommendation_engine DAGs that the DS team can actually reschedule.

- recommendation_engine seed DAGs are treated as candidate DAGs for schedule changes
- recursively required upstream DAGs are treated as fixed context for dependency and waiting analysis
- additional non-recommendation-engine DAG families can be added later as new controllable scopes

Current scoped outputs:

- recommendation_engine runtime summary views in DuckDB
- edge-level wait estimates for mapped recommendation_engine seed dependencies
- generated Markdown report under `artifacts/` for the five reschedulable seed DAGs
- heuristic schedule proposal artifacts for the five reschedulable seed DAGs

## Initial Deliverables

- reusable extraction SQL
- repeatable DuckDB load pipeline
- documented optimizer input tables
- baseline scheduling algorithms
