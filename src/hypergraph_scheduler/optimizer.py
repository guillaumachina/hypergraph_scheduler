from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path

from hypergraph_scheduler.paths import ARTIFACTS_DIR, RECOMMENDATION_ENGINE_INPUTS_DIR


MODEL_PATH = RECOMMENDATION_ENGINE_INPUTS_DIR / "recommendation_engine_schedule_optimization_model.json"


@dataclass(frozen=True)
class WorkingHours:
    earliest_start_minute: int
    latest_start_minute: int


def parse_hhmm(value: str) -> int:
    hour_str, minute_str = value.split(":", maxsplit=1)
    return int(hour_str) * 60 + int(minute_str)


def load_working_hours() -> WorkingHours:
    model = json.loads(MODEL_PATH.read_text())
    working_hours = model["optimization_defaults"]["working_hours_constraint"]
    return WorkingHours(
        earliest_start_minute=parse_hhmm(working_hours["earliest_start"]),
        latest_start_minute=parse_hhmm(working_hours["latest_start"]),
    )


def parse_cron_hours(schedule_resolved: str) -> tuple[int, list[int], str]:
    minute_field, hour_field, day_of_month, month, day_of_week = schedule_resolved.split()
    return int(minute_field), [int(value) for value in hour_field.split(",")], f"{day_of_month} {month} {day_of_week}"


def format_cron(minute: int, hours: list[int], suffix: str) -> str:
    hour_field = ",".join(str(hour) for hour in hours)
    return f"{minute} {hour_field} {suffix}"


def round_up_to_bucket(minute_of_day: int, bucket_minutes: int) -> int:
    return int(math.ceil(minute_of_day / bucket_minutes) * bucket_minutes)


def round_down_to_bucket(minute_of_day: int, bucket_minutes: int) -> int:
    return int(math.floor(minute_of_day / bucket_minutes) * bucket_minutes)


def format_minute_of_day(minute_of_day: int) -> str:
    hour = minute_of_day // 60
    minute = minute_of_day % 60
    return f"{hour:02d}:{minute:02d}"


def format_shifted_time(minute_of_day: int, offset_seconds: float | None) -> str:
    if offset_seconds is None:
        return "n/a"
    shifted = minute_of_day + int(round(offset_seconds / 60.0))
    return format_minute_of_day(shifted)


def format_duration_minutes(total_minutes: int) -> str:
    hours, minutes = divmod(max(0, total_minutes), 60)
    if hours and minutes:
        return f"{hours}h {minutes:02d}m"
    if hours:
        return f"{hours}h"
    return f"{minutes}m"


def add_minutes(minute_of_day: int, duration_minutes: int) -> int:
    return minute_of_day + max(0, duration_minutes)


def iter_candidate_slots(working_hours: WorkingHours, bucket_minutes: int) -> list[int]:
    return list(range(working_hours.earliest_start_minute, working_hours.latest_start_minute + 1, bucket_minutes))


def choose_primary_start_slot(
    *,
    current_primary_start_minute: int,
    assigned_effective_starts: list[int],
    working_hours: WorkingHours,
    bucket_minutes: int,
    min_gap_minutes: int,
    finish_deadline_minute: int,
    effective_processing_minutes: int,
    upstream_ready_minute: int,
    post_ready_setup_minutes: int,
) -> tuple[int, int]:
    candidate_slots = iter_candidate_slots(working_hours, bucket_minutes)
    best_primary_slot = candidate_slots[0]
    best_effective_slot = max(best_primary_slot, upstream_ready_minute) + post_ready_setup_minutes
    best_score: float | None = None

    for slot in candidate_slots:
        predicted_effective_start = max(slot, upstream_ready_minute) + post_ready_setup_minutes
        nearest_gap = min((abs(predicted_effective_start - assigned) for assigned in assigned_effective_starts), default=min_gap_minutes)
        gap_violation = max(0, min_gap_minutes - nearest_gap)
        wait_before_ready = max(0, upstream_ready_minute - slot)
        late_after_ready = max(0, slot - upstream_ready_minute)
        schedule_shift = abs(slot - current_primary_start_minute)
        finish_overrun = max(0, predicted_effective_start + effective_processing_minutes - finish_deadline_minute)

        score = (
            10.0 * gap_violation
            + 6.0 * finish_overrun
            + 5.0 * wait_before_ready
            + 1.25 * late_after_ready
            + 0.1 * schedule_shift
        )

        if best_score is None or score < best_score:
            best_score = score
            best_primary_slot = slot
            best_effective_slot = predicted_effective_start

    return best_primary_slot, best_effective_slot


def finalize_output_row(row: dict[str, object]) -> dict[str, object]:
    internal_keys = {
        "current_effective_start_minute",
        "current_primary_start_minute",
        "effective_start_delay_minutes_raw",
        "effective_processing_minutes_raw",
        "typical_processing_minutes_raw",
        "schedule_suffix",
    }
    return {key: value for key, value in row.items() if key not in internal_keys}


def build_recommendation_engine_schedule_proposal(connection) -> Path:
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    markdown_path = ARTIFACTS_DIR / "recommendation_engine_schedule_proposal.md"
    csv_path = ARTIFACTS_DIR / "recommendation_engine_schedule_proposal.csv"

    working_hours = load_working_hours()
    rows = connection.execute(
        """
        SELECT
            dag_id,
            schedule_resolved,
            direct_upstream_dependency_count,
            avg_dag_runtime_seconds,
            p90_dag_runtime_seconds,
            avg_effective_start_delay_seconds,
            p90_effective_start_delay_seconds,
            avg_effective_processing_seconds,
            median_effective_processing_seconds,
            p90_effective_processing_seconds,
            total_scoped_idle_wait_seconds,
            mapped_upstream_idle_wait_seconds,
            mapped_edge_max_p90_idle_wait_seconds,
            mapped_edge_max_avg_ready_seconds,
            mapped_edge_max_p90_ready_seconds,
            mapped_edge_max_avg_sensor_touch_seconds,
            mapped_edge_max_p90_sensor_touch_seconds
        FROM recommendation_engine_optimization_inputs
        WHERE is_reschedulable
        ORDER BY mapped_upstream_idle_wait_seconds DESC, dag_id
        """
    ).fetchall()

    bucket_minutes = 15
    min_gap_minutes = 45
    finish_deadline_minute = 19 * 60
    proposal_rows: list[dict[str, object]] = []
    fixed_rows: list[dict[str, object]] = []
    slotted_rows: list[dict[str, object]] = []

    for row in rows:
        dag_id = row[0]
        schedule_resolved = row[1]
        direct_upstream_dependency_count = row[2]
        avg_dag_runtime_seconds = row[3]
        p90_dag_runtime_seconds = row[4]
        avg_effective_start_delay_seconds = row[5]
        p90_effective_start_delay_seconds = row[6]
        avg_effective_processing_seconds = row[7]
        median_effective_processing_seconds = row[8]
        p90_effective_processing_seconds = row[9]
        total_scoped_idle_wait_seconds = row[10]
        mapped_upstream_idle_wait_seconds = row[11]
        mapped_edge_max_p90_idle_wait_seconds = row[12]
        mapped_edge_max_avg_ready_seconds = row[13]
        mapped_edge_max_p90_ready_seconds = row[14]
        mapped_edge_max_avg_sensor_touch_seconds = row[15]
        mapped_edge_max_p90_sensor_touch_seconds = row[16]

        minute, hours, suffix = parse_cron_hours(schedule_resolved)
        slot_count = len(hours)
        current_primary_start_minute = min(hours) * 60 + minute
        pressure_buffer_minutes = math.ceil((mapped_edge_max_p90_idle_wait_seconds or 0) / 60)
        effective_start_delay_minutes = int(round((avg_effective_start_delay_seconds or 0) / 60.0))
        effective_processing_minutes = int(round((avg_effective_processing_seconds or 0) / 60.0))
        typical_processing_minutes = int(round((median_effective_processing_seconds or avg_effective_processing_seconds or 0) / 60.0))
        current_effective_start_minute = current_primary_start_minute + effective_start_delay_minutes
        if mapped_edge_max_avg_ready_seconds:
            upstream_ready_minute = current_primary_start_minute + int(round(mapped_edge_max_avg_ready_seconds / 60.0))
            post_ready_setup_minutes = max(0, current_effective_start_minute - upstream_ready_minute)
        else:
            upstream_ready_minute = current_effective_start_minute
            post_ready_setup_minutes = 0

        if slot_count > 1:
            fixed_rows.append(
                {
                    "dag_id": dag_id,
                    "current_schedule": schedule_resolved,
                    "proposed_schedule": schedule_resolved,
                    "current_primary_start_utc": format_minute_of_day(current_primary_start_minute),
                    "proposed_primary_start_utc": format_minute_of_day(current_primary_start_minute),
                    "current_effective_start_utc": format_minute_of_day(current_effective_start_minute),
                    "proposed_effective_start_utc": format_minute_of_day(current_effective_start_minute),
                    "estimated_upstream_ready_utc": format_minute_of_day(upstream_ready_minute),
                    "current_wait_before_ready_minutes": max(0, upstream_ready_minute - current_primary_start_minute),
                    "proposed_wait_before_ready_minutes": max(0, upstream_ready_minute - current_primary_start_minute),
                    "current_gap_after_ready_minutes": max(0, current_primary_start_minute - upstream_ready_minute),
                    "proposed_gap_after_ready_minutes": max(0, current_primary_start_minute - upstream_ready_minute),
                    "wait_saved_minutes": 0,
                    "current_estimated_finish_utc": format_minute_of_day(add_minutes(current_effective_start_minute, typical_processing_minutes)),
                    "proposed_estimated_finish_utc": format_minute_of_day(add_minutes(current_effective_start_minute, typical_processing_minutes)),
                    "shift_minutes": 0,
                    "pressure_buffer_minutes": pressure_buffer_minutes,
                    "effective_start_delay_minutes": effective_start_delay_minutes,
                    "post_ready_setup_minutes": post_ready_setup_minutes,
                    "direct_upstream_dependency_count": direct_upstream_dependency_count,
                    "avg_dag_runtime_seconds": round(avg_dag_runtime_seconds or 0, 1),
                    "p90_dag_runtime_seconds": round(p90_dag_runtime_seconds or 0, 1),
                    "avg_effective_start_delay_seconds": round(avg_effective_start_delay_seconds or 0, 1),
                    "p90_effective_start_delay_seconds": round(p90_effective_start_delay_seconds or 0, 1),
                    "avg_effective_processing_seconds": round(avg_effective_processing_seconds or 0, 1),
                    "median_effective_processing_seconds": round(median_effective_processing_seconds or 0, 1),
                    "p90_effective_processing_seconds": round(p90_effective_processing_seconds or 0, 1),
                    "total_scoped_idle_wait_seconds": round(total_scoped_idle_wait_seconds or 0, 1),
                    "mapped_upstream_idle_wait_seconds": round(mapped_upstream_idle_wait_seconds or 0, 1),
                    "mapped_edge_max_p90_idle_wait_seconds": round(mapped_edge_max_p90_idle_wait_seconds or 0, 1),
                    "mapped_edge_max_avg_ready_seconds": round(mapped_edge_max_avg_ready_seconds or 0, 1),
                    "mapped_edge_max_p90_ready_seconds": round(mapped_edge_max_p90_ready_seconds or 0, 1),
                    "mapped_edge_max_avg_sensor_touch_seconds": round(mapped_edge_max_avg_sensor_touch_seconds or 0, 1),
                    "mapped_edge_max_p90_sensor_touch_seconds": round(mapped_edge_max_p90_sensor_touch_seconds or 0, 1),
                    "strategy": "kept_existing_multi_slot_schedule",
                    "current_effective_start_minute": current_effective_start_minute,
                    "effective_start_delay_minutes_raw": effective_start_delay_minutes,
                    "effective_processing_minutes_raw": effective_processing_minutes,
                    "typical_processing_minutes_raw": typical_processing_minutes,
                    "schedule_suffix": suffix,
                }
            )
        else:
            slotted_rows.append(
                {
                    "dag_id": dag_id,
                    "current_schedule": schedule_resolved,
                    "current_primary_start_minute": current_primary_start_minute,
                    "current_effective_start_minute": current_effective_start_minute,
                    "effective_start_delay_minutes_raw": effective_start_delay_minutes,
                    "upstream_ready_minute": upstream_ready_minute,
                    "post_ready_setup_minutes": post_ready_setup_minutes,
                    "schedule_suffix": suffix,
                    "pressure_buffer_minutes": pressure_buffer_minutes,
                    "direct_upstream_dependency_count": direct_upstream_dependency_count,
                    "avg_dag_runtime_seconds": round(avg_dag_runtime_seconds or 0, 1),
                    "p90_dag_runtime_seconds": round(p90_dag_runtime_seconds or 0, 1),
                    "avg_effective_start_delay_seconds": round(avg_effective_start_delay_seconds or 0, 1),
                    "p90_effective_start_delay_seconds": round(p90_effective_start_delay_seconds or 0, 1),
                    "avg_effective_processing_seconds": round(avg_effective_processing_seconds or 0, 1),
                    "median_effective_processing_seconds": round(median_effective_processing_seconds or 0, 1),
                    "p90_effective_processing_seconds": round(p90_effective_processing_seconds or 0, 1),
                    "total_scoped_idle_wait_seconds": round(total_scoped_idle_wait_seconds or 0, 1),
                    "mapped_upstream_idle_wait_seconds": round(mapped_upstream_idle_wait_seconds or 0, 1),
                    "mapped_edge_max_p90_idle_wait_seconds": round(mapped_edge_max_p90_idle_wait_seconds or 0, 1),
                    "mapped_edge_max_avg_ready_seconds": round(mapped_edge_max_avg_ready_seconds or 0, 1),
                    "mapped_edge_max_p90_ready_seconds": round(mapped_edge_max_p90_ready_seconds or 0, 1),
                    "mapped_edge_max_avg_sensor_touch_seconds": round(mapped_edge_max_avg_sensor_touch_seconds or 0, 1),
                    "mapped_edge_max_p90_sensor_touch_seconds": round(mapped_edge_max_p90_sensor_touch_seconds or 0, 1),
                    "effective_processing_minutes_raw": effective_processing_minutes,
                    "typical_processing_minutes_raw": typical_processing_minutes,
                }
            )

    assigned_effective_starts = [row["current_effective_start_minute"] for row in fixed_rows]
    for row in sorted(slotted_rows, key=lambda item: (item["current_effective_start_minute"], -item["mapped_upstream_idle_wait_seconds"], item["dag_id"])):
        proposed_primary_start_minute, proposed_effective_start_minute = choose_primary_start_slot(
            current_primary_start_minute=row["current_primary_start_minute"],
            assigned_effective_starts=assigned_effective_starts,
            working_hours=working_hours,
            bucket_minutes=bucket_minutes,
            min_gap_minutes=min_gap_minutes,
            finish_deadline_minute=finish_deadline_minute,
            effective_processing_minutes=row["effective_processing_minutes_raw"],
            upstream_ready_minute=row["upstream_ready_minute"],
            post_ready_setup_minutes=row["post_ready_setup_minutes"],
        )
        assigned_effective_starts.append(proposed_effective_start_minute)

        proposed_minute = proposed_primary_start_minute % 60
        proposed_hour = proposed_primary_start_minute // 60
        proposed_schedule = format_cron(proposed_minute, [proposed_hour], row["schedule_suffix"])

        proposal_rows.append(
            finalize_output_row({
                "dag_id": row["dag_id"],
                "current_schedule": row["current_schedule"],
                "proposed_schedule": proposed_schedule,
                "current_primary_start_utc": format_minute_of_day(row["current_primary_start_minute"]),
                "proposed_primary_start_utc": format_minute_of_day(proposed_primary_start_minute),
                "current_effective_start_utc": format_minute_of_day(row["current_effective_start_minute"]),
                "proposed_effective_start_utc": format_minute_of_day(proposed_effective_start_minute),
                "estimated_upstream_ready_utc": format_minute_of_day(row["upstream_ready_minute"]),
                "current_wait_before_ready_minutes": max(0, row["upstream_ready_minute"] - row["current_primary_start_minute"]),
                "proposed_wait_before_ready_minutes": max(0, row["upstream_ready_minute"] - proposed_primary_start_minute),
                "current_gap_after_ready_minutes": max(0, row["current_primary_start_minute"] - row["upstream_ready_minute"]),
                "proposed_gap_after_ready_minutes": max(0, proposed_primary_start_minute - row["upstream_ready_minute"]),
                "wait_saved_minutes": max(0, row["upstream_ready_minute"] - row["current_primary_start_minute"]) - max(0, row["upstream_ready_minute"] - proposed_primary_start_minute),
                "current_estimated_finish_utc": format_minute_of_day(add_minutes(row["current_effective_start_minute"], row["typical_processing_minutes_raw"])),
                "proposed_estimated_finish_utc": format_minute_of_day(add_minutes(proposed_effective_start_minute, row["typical_processing_minutes_raw"])),
                "shift_minutes": proposed_primary_start_minute - row["current_primary_start_minute"],
                "pressure_buffer_minutes": row["pressure_buffer_minutes"],
                "effective_start_delay_minutes": row["effective_start_delay_minutes_raw"],
                "post_ready_setup_minutes": row["post_ready_setup_minutes"],
                "direct_upstream_dependency_count": row["direct_upstream_dependency_count"],
                "avg_dag_runtime_seconds": row["avg_dag_runtime_seconds"],
                "p90_dag_runtime_seconds": row["p90_dag_runtime_seconds"],
                "avg_effective_start_delay_seconds": row["avg_effective_start_delay_seconds"],
                "p90_effective_start_delay_seconds": row["p90_effective_start_delay_seconds"],
                "avg_effective_processing_seconds": row["avg_effective_processing_seconds"],
                "median_effective_processing_seconds": row["median_effective_processing_seconds"],
                "p90_effective_processing_seconds": row["p90_effective_processing_seconds"],
                "total_scoped_idle_wait_seconds": row["total_scoped_idle_wait_seconds"],
                "mapped_upstream_idle_wait_seconds": row["mapped_upstream_idle_wait_seconds"],
                "mapped_edge_max_p90_idle_wait_seconds": row["mapped_edge_max_p90_idle_wait_seconds"],
                "mapped_edge_max_avg_ready_seconds": row["mapped_edge_max_avg_ready_seconds"],
                "mapped_edge_max_p90_ready_seconds": row["mapped_edge_max_p90_ready_seconds"],
                "mapped_edge_max_avg_sensor_touch_seconds": row["mapped_edge_max_avg_sensor_touch_seconds"],
                "mapped_edge_max_p90_sensor_touch_seconds": row["mapped_edge_max_p90_sensor_touch_seconds"],
                "strategy": "upstream_ready_slot_search",
            })
        )

    proposal_rows.extend(finalize_output_row(row) for row in fixed_rows)
    proposal_rows.sort(key=lambda item: item["mapped_upstream_idle_wait_seconds"], reverse=True)

    total_wait_saved_minutes = sum(int(proposal["wait_saved_minutes"]) for proposal in proposal_rows)
    rescheduled_count = sum(1 for proposal in proposal_rows if proposal["strategy"] != "kept_existing_multi_slot_schedule")

    with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(proposal_rows[0].keys()))
        writer.writeheader()
        writer.writerows(proposal_rows)

    lines = [
        "# Recommendation Engine Schedule Proposal",
        "",
        "This is a first heuristic schedule proposal for the five DS-owned recommendation_engine DAGs.",
        "It uses the existing working-hours constraint, the static dependency graph, and observed wait pressure from the DuckDB runtime views.",
        "",
        f"Across {rescheduled_count} rescheduled DAGs, the proposal removes about {format_duration_minutes(total_wait_saved_minutes)} of pre-ready waiting time.",
        "",
        "Heuristic rules:",
        f"- working-hours window: {format_minute_of_day(working_hours.earliest_start_minute)}-{format_minute_of_day(working_hours.latest_start_minute)} UTC",
        f"- time bucket size: {bucket_minutes} minutes",
        f"- minimum stagger gap: {min_gap_minutes} minutes",
        "- reschedulable DAGs are treated as effectively starting when create_config begins",
        "- finish by 19:00 UTC is modeled as a strong soft penalty using success-only post-create_config processing time",
        "- timeline runtime bars use the median success-only processing duration as the typical completion estimate",
        "- effective start is estimated as max(proposed cron, estimated upstream-ready time) plus a small observed post-ready setup lag",
        "- waiting before upstream readiness is penalized more heavily than starting shortly after readiness",
        "- dependency pressure buffer derived from mapped edge P90 idle wait",
        "- multi-slot schedules are currently kept unchanged",
        "",
        "## Waiting Saved",
        "",
        "| DAG | Current wait before ready | Proposed wait before ready | Waiting saved | Estimated upstream ready UTC |",
        "| --- | ---: | ---: | ---: | --- |",
    ]

    for proposal in proposal_rows:
        lines.append(
            "| {} | {} | {} | {} | {} |".format(
                proposal["dag_id"],
                format_duration_minutes(int(proposal["current_wait_before_ready_minutes"])),
                format_duration_minutes(int(proposal["proposed_wait_before_ready_minutes"])),
                format_duration_minutes(int(proposal["wait_saved_minutes"])),
                proposal["estimated_upstream_ready_utc"],
            )
        )

    lines.extend(
        [
            "",
            "## Chronological Graph",
            "",
            "```mermaid",
            "gantt",
            "    title Recommendation Engine Current vs Proposed Timing",
            "    dateFormat HH:mm",
            "    axisFormat %H:%M",
        ]
    )

    for proposal in proposal_rows:
        current_start = proposal["current_primary_start_utc"]
        proposed_start = proposal["proposed_primary_start_utc"]
        ready_time = proposal["estimated_upstream_ready_utc"]
        current_effective = proposal["current_effective_start_utc"]
        proposed_effective = proposal["proposed_effective_start_utc"]
        current_wait = int(proposal["current_wait_before_ready_minutes"])
        proposed_wait = int(proposal["proposed_wait_before_ready_minutes"])
        current_gap_after_ready = int(proposal["current_gap_after_ready_minutes"])
        proposed_gap_after_ready = int(proposal["proposed_gap_after_ready_minutes"])
        processing_minutes = int(round(float(proposal["median_effective_processing_seconds"]) / 60.0))
        wait_saved = int(proposal["wait_saved_minutes"])

        current_lines = [
            f"    section {proposal['dag_id']} current",
            f"    Current wait ({format_duration_minutes(current_wait)}) :crit, {current_start}, {current_wait}m",
            f"    Current estimated run ({format_duration_minutes(processing_minutes)}) :active, {current_effective}, {max(processing_minutes, 1)}m",
        ]
        if current_gap_after_ready > 0:
            current_lines.insert(
                2,
                f"    Current post-ready gap ({format_duration_minutes(current_gap_after_ready)}) :done, {ready_time}, {current_gap_after_ready}m",
            )

        proposed_lines = [
            f"    section {proposal['dag_id']} proposed",
            f"    Proposed wait ({format_duration_minutes(proposed_wait)}) :active, {proposed_start}, {proposed_wait}m",
            f"    Proposed estimated run ({format_duration_minutes(processing_minutes)}) :active, {proposed_effective}, {max(processing_minutes, 1)}m",
            f"    Wait saved ({format_duration_minutes(wait_saved)}) :done, {proposed_start}, {max(wait_saved, 1)}m",
        ]
        if proposed_gap_after_ready > 0:
            proposed_lines.insert(
                2,
                f"    Proposed post-ready gap ({format_duration_minutes(proposed_gap_after_ready)}) :done, {ready_time}, {proposed_gap_after_ready}m",
            )

        lines.extend(current_lines + proposed_lines)

    lines.extend(
        [
            "    section utc scale",
            "    00:00 UTC anchor :done, 00:00, 1m",
        ]
    )

    lines.extend(
        [
            "```",
            "",
            "## Schedule Details",
            "",
            "| DAG | Current schedule | Proposed schedule | Current cron start UTC | Proposed cron start UTC | Estimated upstream ready UTC | Proposed gap after ready | Typical current finish UTC | Typical proposed finish UTC | Waiting saved | Shift min | Post-ready setup min | Pressure buffer min | Strategy |",
            "| --- | --- | --- | --- | --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | --- |",
        ]
    )

    for proposal in proposal_rows:
        lines.append(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                proposal["dag_id"],
                proposal["current_schedule"],
                proposal["proposed_schedule"],
                proposal["current_primary_start_utc"],
                proposal["proposed_primary_start_utc"],
                proposal["estimated_upstream_ready_utc"],
                format_duration_minutes(int(proposal["proposed_gap_after_ready_minutes"])),
                proposal["current_estimated_finish_utc"],
                proposal["proposed_estimated_finish_utc"],
                proposal["wait_saved_minutes"],
                proposal["shift_minutes"],
                proposal["post_ready_setup_minutes"],
                proposal["pressure_buffer_minutes"],
                proposal["strategy"],
            )
        )

    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {markdown_path}")
    print(f"wrote {csv_path}")
    return markdown_path