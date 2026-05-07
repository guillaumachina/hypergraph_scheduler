from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import subprocess

from hypergraph_scheduler.paths import RAW_DATA_DIR, SQL_DIR


@dataclass(frozen=True)
class ExportJob:
    name: str
    sql_file: Path
    output_dir: Path
    output_file: str = "export.csv"

    @property
    def output_path(self) -> Path:
        return self.output_dir / self.output_file


EXPORT_JOBS = (
    ExportJob("dag_run", SQL_DIR / "extract" / "dag_runtime_extract.sql", RAW_DATA_DIR / "dag_run"),
    ExportJob("task_instance", SQL_DIR / "extract" / "task_instance_extract.sql", RAW_DATA_DIR / "task_instance"),
    ExportJob("task_reschedule", SQL_DIR / "extract" / "task_reschedule_extract.sql", RAW_DATA_DIR / "task_reschedule"),
)


def build_psql_copy_script(query: str) -> str:
    normalized_query = query.strip().rstrip(";")
    return (
        "\\set ON_ERROR_STOP on\n"
        f"COPY ({normalized_query}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);\n"
    )


def run_export_job(
    job: ExportJob,
    *,
    host: str,
    port: int,
    database: str,
    user: str,
    sslmode: str,
) -> None:
    job.output_dir.mkdir(parents=True, exist_ok=True)
    copy_script = build_psql_copy_script(job.sql_file.read_text())
    temp_output_path = job.output_path.with_suffix(f"{job.output_path.suffix}.partial")

    env = os.environ.copy()
    env.setdefault("PGSSLMODE", sslmode)

    command = [
        "psql",
        "--host",
        host,
        "--port",
        str(port),
        "--dbname",
        database,
        "--username",
        user,
        "--file",
        "-",
    ]

    try:
        with temp_output_path.open("w", encoding="utf-8") as output_handle:
            subprocess.run(
                command,
                input=copy_script,
                text=True,
                env=env,
                stdout=output_handle,
                check=True,
            )
        temp_output_path.replace(job.output_path)
    finally:
        if temp_output_path.exists():
            temp_output_path.unlink()

    output_size = job.output_path.stat().st_size
    print(f"exported {job.name} -> {job.output_path} ({output_size} bytes)")


def export_all(
    *,
    host: str,
    port: int,
    database: str,
    user: str,
    sslmode: str,
    selected_jobs: list[str] | None = None,
) -> None:
    jobs = EXPORT_JOBS
    if selected_jobs:
        selected = set(selected_jobs)
        jobs = tuple(job for job in EXPORT_JOBS if job.name in selected)

    for job in jobs:
        run_export_job(
            job,
            host=host,
            port=port,
            database=database,
            user=user,
            sslmode=sslmode,
        )