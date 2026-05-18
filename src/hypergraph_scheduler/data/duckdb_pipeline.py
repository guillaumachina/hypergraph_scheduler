import json
from pathlib import Path

import duckdb
import pandas as pd

from hypergraph_scheduler.config import DuckDBConfig, DEFAULT_DUCKDB_CONFIG
from hypergraph_scheduler.paths import RAW_DATA_DIR, SQL_DIR
from hypergraph_scheduler.scopes import ScopeDefinition, discover_scopes


def connect(config: DuckDBConfig = DEFAULT_DUCKDB_CONFIG) -> duckdb.DuckDBPyConnection:
    config.database_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(config.database_path))


def create_raw_table_from_path(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    input_dir: Path,
) -> None:
    parquet_glob = input_dir / "*.parquet"
    csv_glob = input_dir / "*.csv"

    if any(input_dir.glob("*.parquet")):
        connection.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet(?)",
            [str(parquet_glob)],
        )
        return

    if any(input_dir.glob("*.csv")):
        connection.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto(?)",
            [str(csv_glob)],
        )
        return

    raise FileNotFoundError(f"No parquet or csv files found in {input_dir}")


def load_raw_exports(connection: duckdb.DuckDBPyConnection) -> None:
    create_raw_table_from_path(connection, "raw_dag_run", RAW_DATA_DIR / "dag_run")
    create_raw_table_from_path(connection, "raw_task_instance", RAW_DATA_DIR / "task_instance")
    create_raw_table_from_path(connection, "raw_task_reschedule", RAW_DATA_DIR / "task_reschedule")


def replace_table_from_dataframe(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    dataframe: pd.DataFrame,
) -> None:
    connection.register("temp_frame", dataframe)
    connection.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_frame")
    connection.unregister("temp_frame")


def load_scope_static_inputs(connection: duckdb.DuckDBPyConnection, scope: ScopeDefinition) -> None:
    graph = json.loads(scope.graph_path.read_text(encoding="utf-8"))
    model = json.loads(scope.model_path.read_text(encoding="utf-8"))

    replace_table_from_dataframe(
        connection,
        scope.raw_table_name("graph_nodes"),
        pd.DataFrame(graph["nodes"]),
    )
    replace_table_from_dataframe(
        connection,
        scope.raw_table_name("graph_edges"),
        pd.DataFrame(graph["edges"]),
    )

    optimization_rows: list[dict[str, object]] = []
    for dag in model["dags"]:
        constraints: dict[str, object] = dag.get("constraints") or {}
        optimization_rows.append(
            {
                "dag_id": dag["dag_id"],
                "repo": dag.get("repo"),
                "category": dag.get("category"),
                "scheduled_cron": dag.get("scheduled_cron"),
                "fixed_schedule": constraints.get("fixed_schedule"),
            }
        )

    replace_table_from_dataframe(
        connection,
        scope.raw_table_name("optimization_dags"),
        pd.DataFrame(optimization_rows),
    )

    seed_edge_sensor_map = pd.DataFrame(
        scope.seed_edge_sensor_map,
        columns=["from_dag_id", "to_dag_id", "sensor_task_id"],
    )
    replace_table_from_dataframe(
        connection,
        scope.raw_table_name("seed_edge_sensor_map"),
        seed_edge_sensor_map,
    )


def render_scope_views_sql(scope: ScopeDefinition) -> str:
    template_path = SQL_DIR / "transform" / "build_scope_views.sql"
    return template_path.read_text(encoding="utf-8").replace("__SCOPE__", scope.scope_id)


def build_runtime_views(connection: duckdb.DuckDBPyConnection) -> None:
    runtime_sql_path = SQL_DIR / "transform" / "build_runtime_views.sql"
    connection.execute(runtime_sql_path.read_text(encoding="utf-8"))

    for scope in discover_scopes():
        load_scope_static_inputs(connection, scope)
        connection.execute(render_scope_views_sql(scope))


def initialize_local_database(config: DuckDBConfig = DEFAULT_DUCKDB_CONFIG) -> None:
    with connect(config) as connection:
        load_raw_exports(connection)
        build_runtime_views(connection)

