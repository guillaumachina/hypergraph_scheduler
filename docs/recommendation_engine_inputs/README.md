# Recommendation Engine Input Provenance

`hypergraph_scheduler` depends on four versioned files in this directory:

- `recommendation_engine_dag_dependencies.json`
- `recommendation_engine_schedule_optimization_model.json`
- `dag_schedules_and_dependencies.md`
- `recommendation_engine_schedule_optimization_formulation.md`

## Why They Are Vendored Here

These files were originally maintained with the recommendation_engine dependency modeling work, but `hypergraph_scheduler` reads them during `build-views`.
Keeping committed copies here removes the hidden dependency on a sibling checkout and makes the workflow reproducible from this repository alone.

## What They Contain

- `recommendation_engine_dag_dependencies.json`: normalized machine-readable graph of the scoped DAG nodes and edges.
- `recommendation_engine_schedule_optimization_model.json`: scheduling-oriented model containing DAG attributes, optimization defaults, and precedence constraints.
- `dag_schedules_and_dependencies.md`: human-readable inventory of schedules, dependencies, and the recursive upstream expansion used to build the model.
- `recommendation_engine_schedule_optimization_formulation.md`: mathematical formulation and design rationale for the scheduling problem.

## How They Were Extracted

Per the upstream documentation, these inputs were generated from the recommendation_engine DAG definitions under `hedwig/dags/**` and the recursively referenced upstream DAGs found in:

- `hummus`
- `dwh`
- `bi_reports`
- `orders_forecast`
- `menu_creation`

The extraction logic identifies explicit inter-DAG waits such as `ExternalTaskSensor` and `HedwigExternalTaskSensor`, expands the recursively required upstream context, and then materializes both a human-readable dependency inventory and normalized JSON models.

## Refreshing The Vendored Copies

If the recommendation_engine dependency model changes, regenerate these files by rediscovering the recommendation_engine DAG hypergraph and its recursively required upstream context, then refresh the vendored copies in this directory.

At minimum, refresh:

- `recommendation_engine_dag_dependencies.json`
- `recommendation_engine_schedule_optimization_model.json`

If the supporting reference material changes as well, refresh these too:

- `dag_schedules_and_dependencies.md`
- `recommendation_engine_schedule_optimization_formulation.md`

After refreshing them, rebuild the local DuckDB views:

```bash
python -m hypergraph_scheduler build-views
```