# Monday DS Input Provenance

`hypergraph_scheduler` discovers this scope from `scope.json` and depends on the versioned files in this directory:

- `scope.json`
- `monday_ds_dag_dependencies.json`
- `monday_ds_schedule_optimization_model.json`
- `dag_schedules_and_dependencies.md`

## Why They Are Vendored Here

These files capture the Monday-oriented DS DAG hypergraph across the relevant DS repos without requiring live cross-repo re-discovery each time the scheduler is run.
Keeping committed copies here makes the Monday scope reproducible from this repository alone.

## What They Contain

- `scope.json`: scope metadata used to discover this input directory, define the display name, and map the Monday seed edges to the sensor task ids used for wait-pressure analysis.
- `monday_ds_dag_dependencies.json`: normalized machine-readable graph of the Monday-running DS DAGs and their recursively required upstream context.
- `monday_ds_schedule_optimization_model.json`: scheduling-oriented model containing DAG attributes, precedence constraints, and optimization defaults for the Monday scope.
- `dag_schedules_and_dependencies.md`: human-readable inventory of schedules, dependencies, and the recursive upstream expansion used to build the model.

## How They Were Extracted

These inputs were derived from the Monday-running DS DAG definitions under `hedwig/dags/**` and the recursively referenced upstream DAGs found in:

- `recommendation_engine`
- `marketing_engine`
- `orders_forecast`
- `sales_forecast`
- `menu_creation`
- `hummus`
- `dwh`
- `bi_reports`

The extraction logic follows the same static approach as the recommendation_engine scope:

- identify explicit inter-DAG waits from `ExternalTaskSensor` and `HedwigExternalTaskSensor`
- expand the recursively required upstream context
- preserve schedule alignment details such as `execution_delta`
- keep disabled-but-relevant DAGs visible in the human-readable graph
- materialize both a human-readable dependency inventory and normalized JSON models
