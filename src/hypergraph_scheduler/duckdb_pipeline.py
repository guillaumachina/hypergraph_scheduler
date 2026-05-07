from pathlib import Path

import duckdb

from hypergraph_scheduler.config import DuckDBConfig, DEFAULT_DUCKDB_CONFIG
from hypergraph_scheduler.paths import RAW_DATA_DIR, SQL_DIR


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


def build_runtime_views(connection: duckdb.DuckDBPyConnection) -> None:
    runtime_sql_path = SQL_DIR / "transform" / "build_runtime_views.sql"
    connection.execute(runtime_sql_path.read_text())


def initialize_local_database(config: DuckDBConfig = DEFAULT_DUCKDB_CONFIG) -> None:
    with connect(config) as connection:
        load_raw_exports(connection)
        build_runtime_views(connection)

