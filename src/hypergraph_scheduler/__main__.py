import argparse

from hypergraph_scheduler.duckdb_pipeline import build_runtime_views, connect, load_raw_exports


def main() -> None:
    parser = argparse.ArgumentParser(description="Local DuckDB workflow for Airflow metadata analysis")
    parser.add_argument(
        "command",
        choices=["load-raw", "build-views", "init-db"],
        help="Workflow command to run",
    )
    args = parser.parse_args()

    with connect() as connection:
        if args.command == "load-raw":
            load_raw_exports(connection)
        elif args.command == "build-views":
            build_runtime_views(connection)
        elif args.command == "init-db":
            load_raw_exports(connection)
            build_runtime_views(connection)

if __name__ == "__main__":
    main()