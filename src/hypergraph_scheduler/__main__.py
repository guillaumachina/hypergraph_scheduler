import argparse

from hypergraph_scheduler.duckdb_pipeline import build_runtime_views, connect, load_raw_exports
from hypergraph_scheduler.export_raw import export_all
from hypergraph_scheduler.reporting import build_recommendation_engine_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Local DuckDB workflow for Airflow metadata analysis")
    parser.add_argument(
        "command",
        choices=["export-raw", "load-raw", "build-views", "build-report", "init-db"],
        help="Workflow command to run",
    )
    parser.add_argument("--host", help="PostgreSQL host for export-raw")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port for export-raw")
    parser.add_argument("--database", help="PostgreSQL database name for export-raw")
    parser.add_argument("--user", help="PostgreSQL user for export-raw")
    parser.add_argument(
        "--sslmode",
        default="prefer",
        help="PostgreSQL sslmode for export-raw",
    )
    parser.add_argument(
        "--jobs",
        nargs="+",
        choices=["dag_run", "task_instance", "task_reschedule"],
        help="Subset of raw export jobs to run for export-raw",
    )
    args = parser.parse_args()

    if args.command == "export-raw":
        missing = [
            name
            for name, value in {
                "--host": args.host,
                "--database": args.database,
                "--user": args.user,
            }.items()
            if not value
        ]
        if missing:
            parser.error(f"export-raw requires {' '.join(missing)}")

        export_all(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            sslmode=args.sslmode,
            selected_jobs=args.jobs,
        )
        return

    with connect() as connection:
        if args.command == "load-raw":
            load_raw_exports(connection)
        elif args.command == "build-views":
            build_runtime_views(connection)
        elif args.command == "build-report":
            build_recommendation_engine_report(connection)
        elif args.command == "init-db":
            load_raw_exports(connection)
            build_runtime_views(connection)

if __name__ == "__main__":
    main()