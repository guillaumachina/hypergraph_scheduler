from pathlib import Path

from hypergraph_scheduler.paths import ARTIFACTS_DIR


def _format_seconds(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.1f}"


def _format_hours(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value / 3600.0:,.2f}"


def build_recommendation_engine_report(connection) -> Path:
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = ARTIFACTS_DIR / "recommendation_engine_candidate_report.md"

    candidate_rows = connection.execute(
        """
        SELECT
            dag_id,
            schedule_resolved,
            scheduled_run_count,
            avg_dag_runtime_seconds,
            p90_dag_runtime_seconds,
            avg_effective_start_delay_seconds,
            p90_effective_start_delay_seconds,
            total_scoped_idle_wait_seconds,
            mapped_upstream_idle_wait_seconds,
            mapped_edge_max_p90_idle_wait_seconds,
            mapped_edge_max_avg_sensor_touch_seconds,
            direct_upstream_dependency_count
        FROM recommendation_engine_candidate_report
        ORDER BY mapped_upstream_idle_wait_seconds DESC, dag_id
        """
    ).fetchall()

    edge_rows = connection.execute(
        """
        SELECT
            from_dag_id,
            to_dag_id,
            sensor_task_id,
            sensor_run_count,
            total_idle_wait_seconds,
            p90_idle_wait_seconds
        FROM recommendation_engine_seed_edge_waits
        ORDER BY COALESCE(total_idle_wait_seconds, 0) DESC, to_dag_id, from_dag_id
        """
    ).fetchall()

    lines = [
        "# Recommendation Engine Rescheduling Report",
        "",
        "This report scopes optimization candidates to DS-owned recommendation_engine DAGs and treats upstream DAGs as fixed context.",
        "",
        "## Candidate Summary",
        "",
        "| DAG | Schedule | Runs | Avg runtime s | P90 runtime s | Avg effective start h | P90 effective start h | Total sensor idle wait s | Mapped upstream idle wait s | Max mapped edge P90 wait s | Max mapped sensor touch h | Direct upstream deps |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for row in candidate_rows:
        lines.append(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                row[0],
                row[1],
                row[2],
                _format_seconds(row[3]),
                _format_seconds(row[4]),
                _format_hours(row[5]),
                _format_hours(row[6]),
                _format_seconds(row[7]),
                _format_seconds(row[8]),
                _format_seconds(row[9]),
                _format_hours(row[10]),
                row[11],
            )
        )

    lines.extend(
        [
            "",
            "## Edge-Level Wait Pressure",
            "",
            "| Upstream DAG | Downstream DAG | Sensor task | Runs | Total idle wait s | P90 idle wait s |",
            "| --- | --- | --- | ---: | ---: | ---: |",
        ]
    )

    for row in edge_rows:
        lines.append(
            "| {} | {} | {} | {} | {} | {} |".format(
                row[0],
                row[1],
                row[2] or "n/a",
                row[3] if row[3] is not None else 0,
                _format_seconds(row[4]),
                _format_seconds(row[5]),
            )
        )

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {output_path}")
    return output_path