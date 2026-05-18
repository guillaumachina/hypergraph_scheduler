from __future__ import annotations

from collections import defaultdict
import csv
import math
from statistics import median
from pathlib import Path

import duckdb

from hypergraph_scheduler.data.data_loaders import (
    coerce_datetime as _coerce_datetime,
    coerce_float as _coerce_float,
    load_global_pressure_profile,
    load_observed_effective_start_profile,
    load_observed_global_task_peak,
    load_observed_per_dag_run_peaks,
    load_observed_per_dag_task_peaks,
    load_observed_per_dag_task_peak_profiles,
    load_observed_scoped_task_peak,
    load_observed_task_peak_profile,
    load_recent_observed_effective_start_minutes,
    load_task_intervals_by_dag,
    load_task_intervals_for_profiles,
    load_task_count_estimates,
    load_task_sum_estimates,
)
from hypergraph_scheduler.scheduling.models import (
    DEFAULT_TASK_SUM_EXCLUDED_OPERATOR_PATTERNS,
    DEFAULT_TASK_SUM_EXCLUDED_TASK_PATTERNS,
    GlobalPressureProfile,
    ObservedPeak,
    OptimizationInputRow,
    ProposalRow,
    RepresentativeRunProfile,
    RepresentativeRunRow,
    RuntimeEstimationConfig,
    SlottedDagPlanInput,
    TaskCountEstimate,
    TaskSumEstimate,
    WorkingHours,
)
from hypergraph_scheduler.paths import ARTIFACTS_DIR
from hypergraph_scheduler.proposal.proposal_analysis import (
    build_exact_shifted_peak_task_series as _build_exact_shifted_peak_task_series,
    build_scoped_parallel_task_series as _build_scoped_parallel_task_series,
    build_scoped_peak_task_series as _build_scoped_peak_task_series,
)
from hypergraph_scheduler.proposal.proposal_document import (
    render_reviewed_assumptions_markdown,
    render_schedule_proposal_markdown,
)
from hypergraph_scheduler.proposal.proposal_outputs import (
    append_hourly_delta_summary as _append_hourly_delta_summary,
    append_hourly_table as _append_hourly_table,
    build_combined_hourly_xychart as _build_combined_hourly_xychart,
    build_global_pressure_xychart as _build_global_pressure_xychart,
    hourly_average_series as _hourly_average_series,
    hourly_peak_series as _hourly_peak_series,
    hourly_peak_slot_series as _hourly_peak_slot_series,
)
from hypergraph_scheduler.scheduling.runtime_estimation import (
    choose_recommender_processing_seconds,
    build_replay_profile,
    choose_representative_run as _choose_representative_run,
    choose_typical_runtime_seconds,
    load_optimization_model,
    load_runtime_estimation_config,
    load_solver_config,
    load_working_hours,
    profile_completion_minutes as _profile_completion_minutes,
    profile_processing_minutes as _profile_processing_minutes,
    profile_start_delay_minutes as _profile_start_delay_minutes,
    proposal_effective_window_minutes as _proposal_effective_window_minutes,
)
from hypergraph_scheduler.scheduling.schedule_solver import solve_slotted_rows
from hypergraph_scheduler.scopes import ScopeDefinition, get_scope
from hypergraph_scheduler.scheduling.slot_optimization import (
    average_global_pressure_for_window,
    choose_primary_start_slot,
    task_load_weight as _task_load_weight,
)
from hypergraph_scheduler.scheduling.time_utils import (
    add_minutes,
    format_cron,
    format_duration_minutes,
    format_minute_of_day,
    format_shifted_time,
    parse_cron_hours,
    parse_hhmm,
    round_up_to_bucket,
)


def _limit_status(observed_peak: int, limit_value: int) -> str:
    if observed_peak < limit_value:
        return "below_limit"
    if observed_peak == limit_value:
        return "at_limit"
    return "above_limit"


def _estimate_upstream_ready_minute(
    *,
    current_primary_start_minute: int,
    current_effective_start_minute: int,
    mapped_edge_max_median_clipped_ready_seconds: float | None,
    post_ready_setup_minutes: int,
    recent_observed_effective_start_minute: int | None,
) -> int:
    edge_ready_minute = None
    if mapped_edge_max_median_clipped_ready_seconds:
        edge_ready_minute = current_primary_start_minute + int(round(mapped_edge_max_median_clipped_ready_seconds / 60.0))

    readiness_caps = []
    observed_ready_from_current = max(current_primary_start_minute, current_effective_start_minute - post_ready_setup_minutes)
    readiness_caps.append(observed_ready_from_current)
    if recent_observed_effective_start_minute is not None:
        readiness_caps.append(max(current_primary_start_minute, recent_observed_effective_start_minute - post_ready_setup_minutes))

    if edge_ready_minute is not None:
        return min(edge_ready_minute, min(readiness_caps))
    return min(readiness_caps)


def _build_reviewed_assumption_row(
    *,
    dag_id: str,
    current_schedule: str,
    slot_count: int,
    manual_override_seconds: float | None,
    effective_processing_minutes: int,
    upstream_ready_minute: int,
    dependency_gate_offset_minutes: int,
    post_ready_setup_minutes: int,
    force_earliest_ready_slot: bool,
    is_dependency_gated: bool,
    is_sequenced: bool,
    dag_notes: list[str],
) -> dict[str, object]:
    runtime_source = "manual_reviewed" if manual_override_seconds is not None and manual_override_seconds > 0 else "history_fallback"
    if force_earliest_ready_slot:
        upstream_ready_source = "reviewed_ready_start_rule"
    elif is_dependency_gated or is_sequenced:
        upstream_ready_source = "reviewed_dependency_rule"
    else:
        upstream_ready_source = "history_inferred"
    if manual_override_seconds is not None and manual_override_seconds > 0:
        confidence = "reviewed_assumption"
    elif slot_count > 1:
        confidence = "hard_fact"
    elif is_dependency_gated or is_sequenced or force_earliest_ready_slot:
        confidence = "reviewed_assumption"
    else:
        confidence = "advisory_history"
    note_fragments = list(dag_notes)
    if force_earliest_ready_slot:
        note_fragments.append("start at first reviewed ready slot")
    if is_dependency_gated:
        note_fragments.append("mid-run dependency gate enforced")
    if is_sequenced:
        note_fragments.append("explicit sequencing rule enforced")

    return {
        "dag_id": dag_id,
        "current_schedule": current_schedule,
        "movability": "fixed_multi_slot" if slot_count > 1 else "reschedulable_single_slot",
        "runtime_source": runtime_source,
        "reviewed_runtime": format_duration_minutes(effective_processing_minutes),
        "reviewed_runtime_minutes": effective_processing_minutes,
        "upstream_ready_utc": format_minute_of_day(upstream_ready_minute),
        "upstream_ready_source": upstream_ready_source,
        "dependency_gate": format_duration_minutes(dependency_gate_offset_minutes),
        "dependency_gate_minutes": dependency_gate_offset_minutes,
        "post_ready_setup": format_duration_minutes(post_ready_setup_minutes),
        "post_ready_setup_minutes": post_ready_setup_minutes,
        "confidence": confidence,
        "notes": " | ".join(fragment for fragment in note_fragments if fragment),
    }


def build_scope_schedule_proposal(
    connection: duckdb.DuckDBPyConnection,
    scope: ScopeDefinition,
    solver_backend: str | None = None,
    solver_objective_mode: str | None = None,
) -> Path:
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    markdown_path = ARTIFACTS_DIR / f"{scope.artifact_prefix}_schedule_proposal.md"
    csv_path = ARTIFACTS_DIR / f"{scope.artifact_prefix}_schedule_proposal.csv"
    reviewed_assumptions_csv_path = ARTIFACTS_DIR / f"{scope.artifact_prefix}_reviewed_assumptions.csv"
    reviewed_assumptions_markdown_path = ARTIFACTS_DIR / f"{scope.artifact_prefix}_reviewed_assumptions.md"
    mermaid_chart_path = ARTIFACTS_DIR / f"{scope.artifact_prefix}_pressure_parallel_evolution.mmd"
    global_mermaid_chart_path = ARTIFACTS_DIR / f"{scope.artifact_prefix}_global_pressure_evolution.mmd"

    optimization_model = load_optimization_model(scope.model_path)
    optimization_defaults = optimization_model.get("optimization_defaults", {})
    if not isinstance(optimization_defaults, dict):
        optimization_defaults = {}
    output_config = optimization_defaults.get("output", {})
    if not isinstance(output_config, dict):
        output_config = {}
    reviewed_assumptions_first = bool(output_config.get("reviewed_assumptions_first", scope.scope_id == "monday_ds"))
    working_hours = load_working_hours(scope.model_path)
    runtime_estimation_config = load_runtime_estimation_config(scope.model_path)
    solver_config = load_solver_config(
        scope.model_path,
        backend_override=solver_backend,
        objective_mode_override=solver_objective_mode,
    )
    rows: list[OptimizationInputRow] = connection.execute(
        f"""
        SELECT
            dag_id,
            schedule_resolved,
            direct_upstream_dependency_count,
            avg_dag_runtime_seconds,
            median_dag_runtime_seconds,
            p90_dag_runtime_seconds,
            median_schedule_to_end_seconds,
            avg_effective_start_delay_seconds,
            p90_effective_start_delay_seconds,
            avg_effective_processing_seconds,
            median_effective_processing_seconds,
            p90_effective_processing_seconds,
            total_scoped_idle_wait_seconds,
            mapped_upstream_idle_wait_seconds,
            mapped_edge_max_p90_idle_wait_seconds,
            mapped_edge_max_avg_ready_seconds,
            mapped_edge_max_median_clipped_ready_seconds,
            mapped_edge_max_p90_ready_seconds,
            mapped_edge_max_avg_sensor_touch_seconds,
            mapped_edge_max_p90_sensor_touch_seconds
        FROM {scope.view_name('optimization_inputs')}
        WHERE is_reschedulable
        ORDER BY mapped_upstream_idle_wait_seconds DESC, dag_id
        """
    ).fetchall()

    bucket_minutes = 15
    min_gap_minutes = 45
    finish_deadline_minute = 19 * 60
    proposal_rows: list[ProposalRow] = []
    fixed_rows: list[ProposalRow] = []
    slotted_rows: list[SlottedDagPlanInput] = []
    assigned_effective_starts: list[int] = []
    assigned_load_windows: list[tuple[int, int, float, float] | tuple[int, int, float, float, GlobalPressureProfile]] = []
    reviewed_assumption_rows: list[dict[str, object]] = []
    scoped_dag_ids = {str(row[0]) for row in rows}
    dag_entries = optimization_model.get("dags", [])
    dag_metadata_by_id = {
        str(entry.get("dag_id")): entry
        for entry in dag_entries
        if isinstance(entry, dict) and entry.get("dag_id")
    }
    task_count_estimates = load_task_count_estimates(connection, scoped_dag_ids)
    recent_observed_effective_start_minutes = load_recent_observed_effective_start_minutes(connection, scoped_dag_ids)
    global_pressure_by_minute = load_global_pressure_profile(connection, bucket_minutes)
    observed_global_peak_profile = load_observed_task_peak_profile(connection, bucket_minutes)
    observed_global_effective_start_profile = load_observed_effective_start_profile(connection, bucket_minutes)
    observed_per_dag_task_peaks = load_observed_per_dag_task_peaks(connection, scoped_dag_ids)
    observed_per_dag_task_peak_profiles = load_observed_per_dag_task_peak_profiles(connection, bucket_minutes, scoped_dag_ids)
    observed_non_scoped_peak_profile = load_observed_task_peak_profile(connection, bucket_minutes, exclude_dag_ids=scoped_dag_ids)
    observed_non_scoped_effective_start_profile = load_observed_effective_start_profile(
        connection,
        bucket_minutes,
        exclude_dag_ids=scoped_dag_ids,
    )

    for row in rows:
        dag_id = row[0]
        schedule_resolved = row[1]
        direct_upstream_dependency_count = row[2]
        avg_dag_runtime_seconds = row[3]
        median_dag_runtime_seconds = row[4]
        p90_dag_runtime_seconds = row[5]
        median_schedule_to_end_seconds = row[6]
        avg_effective_start_delay_seconds = row[7]
        p90_effective_start_delay_seconds = row[8]
        avg_effective_processing_seconds = row[9]
        median_effective_processing_seconds = row[10]
        p90_effective_processing_seconds = row[11]
        total_scoped_idle_wait_seconds = row[12]
        mapped_upstream_idle_wait_seconds = row[13]
        mapped_edge_max_p90_idle_wait_seconds = row[14]
        mapped_edge_max_avg_ready_seconds = row[15]
        mapped_edge_max_median_clipped_ready_seconds = row[16]
        mapped_edge_max_p90_ready_seconds = row[17]
        mapped_edge_max_avg_sensor_touch_seconds = row[18]
        mapped_edge_max_p90_sensor_touch_seconds = row[19]

        minute, hours, suffix = parse_cron_hours(schedule_resolved)
        slot_count = len(hours)
        current_primary_start_minute = min(hours) * 60 + minute
        pressure_buffer_minutes = math.ceil((mapped_edge_max_p90_idle_wait_seconds or 0) / 60)
        effective_start_delay_minutes = int(round((avg_effective_start_delay_seconds or 0) / 60.0))
        manual_override_seconds = runtime_estimation_config.manual_overrides_seconds.get(dag_id)
        typical_runtime_seconds = choose_recommender_processing_seconds(
            manual_override_seconds=manual_override_seconds,
            median_effective_processing_seconds=median_effective_processing_seconds,
            avg_effective_processing_seconds=avg_effective_processing_seconds,
            avg_dag_runtime_seconds=avg_dag_runtime_seconds,
            median_dag_runtime_seconds=median_dag_runtime_seconds,
        )
        effective_processing_minutes = int(round(typical_runtime_seconds / 60.0))
        typical_processing_minutes = effective_processing_minutes
        typical_completion_minutes = int(round((median_schedule_to_end_seconds or typical_runtime_seconds) / 60.0))
        current_effective_start_minute = current_primary_start_minute + effective_start_delay_minutes
        if mapped_edge_max_median_clipped_ready_seconds:
            provisional_upstream_ready_minute = current_primary_start_minute + int(
                round(mapped_edge_max_median_clipped_ready_seconds / 60.0)
            )
            ready_delay_minutes = max(0, provisional_upstream_ready_minute - current_primary_start_minute)
            post_ready_setup_minutes = max(
                0,
                typical_completion_minutes - ready_delay_minutes - effective_processing_minutes,
            )
        else:
            post_ready_setup_minutes = 0
        upstream_ready_minute = _estimate_upstream_ready_minute(
            current_primary_start_minute=current_primary_start_minute,
            current_effective_start_minute=current_effective_start_minute,
            mapped_edge_max_median_clipped_ready_seconds=mapped_edge_max_median_clipped_ready_seconds,
            post_ready_setup_minutes=post_ready_setup_minutes,
            recent_observed_effective_start_minute=recent_observed_effective_start_minutes.get(str(dag_id)),
        )
        dependency_gate_offset_minutes = int(
            round(runtime_estimation_config.dependency_gate_offsets_seconds.get(dag_id, 0.0) / 60.0)
        )
        force_earliest_ready_slot = str(dag_id) in solver_config.ready_start_dag_ids
        is_dependency_gated = any(gated_dag_id == str(dag_id) for _, gated_dag_id in solver_config.dependency_gate_pairs)
        is_sequenced = any(dag_id in pair for pair in solver_config.sequential_dag_pairs)
        dag_notes = dag_metadata_by_id.get(str(dag_id), {}).get("notes", [])
        if not isinstance(dag_notes, list):
            dag_notes = []
        reviewed_assumption_rows.append(
            _build_reviewed_assumption_row(
                dag_id=str(dag_id),
                current_schedule=schedule_resolved,
                slot_count=slot_count,
                manual_override_seconds=manual_override_seconds,
                effective_processing_minutes=effective_processing_minutes,
                upstream_ready_minute=upstream_ready_minute,
                dependency_gate_offset_minutes=dependency_gate_offset_minutes,
                post_ready_setup_minutes=post_ready_setup_minutes,
                force_earliest_ready_slot=force_earliest_ready_slot,
                is_dependency_gated=is_dependency_gated,
                is_sequenced=is_sequenced,
                dag_notes=[str(note) for note in dag_notes],
            )
        )

        if slot_count > 1:
            fixed_rows.append(
                ProposalRow(
                    dag_id=dag_id,
                    current_schedule=schedule_resolved,
                    proposed_schedule=schedule_resolved,
                    current_primary_start_utc=format_minute_of_day(current_primary_start_minute),
                    proposed_primary_start_utc=format_minute_of_day(current_primary_start_minute),
                    current_effective_start_utc=format_minute_of_day(current_effective_start_minute),
                    proposed_effective_start_utc=format_minute_of_day(current_effective_start_minute),
                    estimated_upstream_ready_utc=format_minute_of_day(upstream_ready_minute),
                    current_wait_before_ready_minutes=max(0, upstream_ready_minute - current_primary_start_minute),
                    proposed_wait_before_ready_minutes=max(0, upstream_ready_minute - current_primary_start_minute),
                    current_gap_after_ready_minutes=max(0, current_primary_start_minute - upstream_ready_minute),
                    proposed_gap_after_ready_minutes=max(0, current_primary_start_minute - upstream_ready_minute),
                    wait_saved_minutes=0,
                    current_estimated_finish_utc=format_minute_of_day(add_minutes(current_primary_start_minute, typical_completion_minutes)),
                    proposed_estimated_finish_utc=format_minute_of_day(add_minutes(current_primary_start_minute, typical_completion_minutes)),
                    shift_minutes=0,
                    pressure_buffer_minutes=pressure_buffer_minutes,
                    effective_start_delay_minutes=effective_start_delay_minutes,
                    post_ready_setup_minutes=post_ready_setup_minutes,
                    direct_upstream_dependency_count=direct_upstream_dependency_count or 0,
                    avg_dag_runtime_seconds=round(avg_dag_runtime_seconds or 0, 1),
                    median_dag_runtime_seconds=round(median_dag_runtime_seconds or 0, 1),
                    p90_dag_runtime_seconds=round(p90_dag_runtime_seconds or 0, 1),
                    avg_effective_start_delay_seconds=round(avg_effective_start_delay_seconds or 0, 1),
                    p90_effective_start_delay_seconds=round(p90_effective_start_delay_seconds or 0, 1),
                    avg_effective_processing_seconds=round(avg_effective_processing_seconds or 0, 1),
                    median_effective_processing_seconds=round(median_effective_processing_seconds or 0, 1),
                    p90_effective_processing_seconds=round(p90_effective_processing_seconds or 0, 1),
                    total_scoped_idle_wait_seconds=round(total_scoped_idle_wait_seconds or 0, 1),
                    mapped_upstream_idle_wait_seconds=round(mapped_upstream_idle_wait_seconds or 0, 1),
                    mapped_edge_max_p90_idle_wait_seconds=round(mapped_edge_max_p90_idle_wait_seconds or 0, 1),
                    mapped_edge_max_avg_ready_seconds=round(mapped_edge_max_avg_ready_seconds or 0, 1),
                    mapped_edge_max_median_clipped_ready_seconds=round(mapped_edge_max_median_clipped_ready_seconds or 0, 1),
                    mapped_edge_max_p90_ready_seconds=round(mapped_edge_max_p90_ready_seconds or 0, 1),
                    mapped_edge_max_avg_sensor_touch_seconds=round(mapped_edge_max_avg_sensor_touch_seconds or 0, 1),
                    mapped_edge_max_p90_sensor_touch_seconds=round(mapped_edge_max_p90_sensor_touch_seconds or 0, 1),
                    strategy="kept_existing_multi_slot_schedule",
                    recent_observed_effective_start_utc=format_minute_of_day(
                        recent_observed_effective_start_minutes.get(str(dag_id), current_effective_start_minute)
                    ),
                )
            )
            assigned_effective_starts.append(current_effective_start_minute)
            assigned_load_windows.append(
                (
                    current_effective_start_minute,
                    current_effective_start_minute + effective_processing_minutes,
                    _task_load_weight(task_count_estimates.get(str(dag_id))),
                    float(observed_per_dag_task_peaks.get(str(dag_id), ObservedPeak(subject=str(dag_id), observed_peak=0, peak_time="")).observed_peak),
                    observed_per_dag_task_peak_profiles.get(str(dag_id), {}),
                )
            )
        else:
            slotted_rows.append(
                SlottedDagPlanInput(
                    dag_id=dag_id,
                    current_schedule=schedule_resolved,
                    current_primary_start_minute=current_primary_start_minute,
                    current_effective_start_minute=current_effective_start_minute,
                    effective_start_delay_minutes=effective_start_delay_minutes,
                    upstream_ready_minute=upstream_ready_minute,
                    dependency_gate_offset_minutes=dependency_gate_offset_minutes,
                    post_ready_setup_minutes=post_ready_setup_minutes,
                    schedule_suffix=suffix,
                    pressure_buffer_minutes=pressure_buffer_minutes,
                    direct_upstream_dependency_count=direct_upstream_dependency_count or 0,
                    avg_dag_runtime_seconds=round(avg_dag_runtime_seconds or 0, 1),
                    median_dag_runtime_seconds=round(median_dag_runtime_seconds or 0, 1),
                    p90_dag_runtime_seconds=round(p90_dag_runtime_seconds or 0, 1),
                    median_schedule_to_end_seconds=round(median_schedule_to_end_seconds or 0, 1),
                    avg_effective_start_delay_seconds=round(avg_effective_start_delay_seconds or 0, 1),
                    p90_effective_start_delay_seconds=round(p90_effective_start_delay_seconds or 0, 1),
                    avg_effective_processing_seconds=round(avg_effective_processing_seconds or 0, 1),
                    median_effective_processing_seconds=round(median_effective_processing_seconds or 0, 1),
                    p90_effective_processing_seconds=round(p90_effective_processing_seconds or 0, 1),
                    total_scoped_idle_wait_seconds=round(total_scoped_idle_wait_seconds or 0, 1),
                    mapped_upstream_idle_wait_seconds=round(mapped_upstream_idle_wait_seconds or 0, 1),
                    mapped_edge_max_p90_idle_wait_seconds=round(mapped_edge_max_p90_idle_wait_seconds or 0, 1),
                    mapped_edge_max_avg_ready_seconds=round(mapped_edge_max_avg_ready_seconds or 0, 1),
                    mapped_edge_max_median_clipped_ready_seconds=round(mapped_edge_max_median_clipped_ready_seconds or 0, 1),
                    mapped_edge_max_p90_ready_seconds=round(mapped_edge_max_p90_ready_seconds or 0, 1),
                    mapped_edge_max_avg_sensor_touch_seconds=round(mapped_edge_max_avg_sensor_touch_seconds or 0, 1),
                    mapped_edge_max_p90_sensor_touch_seconds=round(mapped_edge_max_p90_sensor_touch_seconds or 0, 1),
                    effective_processing_minutes=effective_processing_minutes,
                    typical_processing_minutes=typical_processing_minutes,
                    median_task_count=task_count_estimates.get(str(dag_id), 0.0),
                    force_earliest_ready_slot=force_earliest_ready_slot,
                )
            )

    solve_result = solve_slotted_rows(
        slotted_rows,
        solver_config=solver_config,
        working_hours=working_hours,
        bucket_minutes=bucket_minutes,
        min_gap_minutes=min_gap_minutes,
        finish_deadline_minute=finish_deadline_minute,
        assigned_effective_starts=assigned_effective_starts,
        assigned_load_windows=assigned_load_windows,
        global_pressure_by_minute=global_pressure_by_minute,
        global_peak_by_minute=observed_non_scoped_peak_profile,
        observed_global_peak_target_by_minute=observed_global_peak_profile,
        background_effective_starts_by_minute=observed_non_scoped_effective_start_profile,
        observed_global_effective_start_target_by_minute=observed_global_effective_start_profile,
        observed_per_dag_task_peaks=observed_per_dag_task_peaks,
        observed_per_dag_task_peak_profiles=observed_per_dag_task_peak_profiles,
    )

    rows_by_dag_id = {row.dag_id: row for row in slotted_rows}
    slotted_assignments = solve_result.assignments
    if solve_result.status == "rejected":
        for row in slotted_rows:
            current_wait_before_ready_minutes = max(0, row.upstream_ready_minute - row.current_primary_start_minute)
            current_gap_after_ready_minutes = max(0, row.current_primary_start_minute - row.upstream_ready_minute)
            current_effective_start_minute = max(row.current_primary_start_minute, row.upstream_ready_minute) + row.post_ready_setup_minutes
            proposal_rows.append(
                ProposalRow(
                    dag_id=row.dag_id,
                    current_schedule=row.current_schedule,
                    proposed_schedule=row.current_schedule,
                    current_primary_start_utc=format_minute_of_day(row.current_primary_start_minute),
                    proposed_primary_start_utc=format_minute_of_day(row.current_primary_start_minute),
                    current_effective_start_utc=format_minute_of_day(current_effective_start_minute),
                    proposed_effective_start_utc=format_minute_of_day(current_effective_start_minute),
                    estimated_upstream_ready_utc=format_minute_of_day(row.upstream_ready_minute),
                    current_wait_before_ready_minutes=current_wait_before_ready_minutes,
                    proposed_wait_before_ready_minutes=current_wait_before_ready_minutes,
                    current_gap_after_ready_minutes=current_gap_after_ready_minutes,
                    proposed_gap_after_ready_minutes=current_gap_after_ready_minutes,
                    wait_saved_minutes=0,
                    current_estimated_finish_utc=format_minute_of_day(add_minutes(row.current_primary_start_minute, int(round(row.median_schedule_to_end_seconds / 60.0)) if row.median_schedule_to_end_seconds else row.typical_processing_minutes)),
                    proposed_estimated_finish_utc=format_minute_of_day(add_minutes(row.current_primary_start_minute, int(round(row.median_schedule_to_end_seconds / 60.0)) if row.median_schedule_to_end_seconds else row.typical_processing_minutes)),
                    shift_minutes=0,
                    pressure_buffer_minutes=row.pressure_buffer_minutes,
                    effective_start_delay_minutes=row.effective_start_delay_minutes,
                    post_ready_setup_minutes=row.post_ready_setup_minutes,
                    direct_upstream_dependency_count=row.direct_upstream_dependency_count,
                    avg_dag_runtime_seconds=row.avg_dag_runtime_seconds,
                    median_dag_runtime_seconds=row.median_dag_runtime_seconds,
                    p90_dag_runtime_seconds=row.p90_dag_runtime_seconds,
                    avg_effective_start_delay_seconds=row.avg_effective_start_delay_seconds,
                    p90_effective_start_delay_seconds=row.p90_effective_start_delay_seconds,
                    avg_effective_processing_seconds=row.avg_effective_processing_seconds,
                    median_effective_processing_seconds=row.median_effective_processing_seconds,
                    p90_effective_processing_seconds=row.p90_effective_processing_seconds,
                    total_scoped_idle_wait_seconds=row.total_scoped_idle_wait_seconds,
                    mapped_upstream_idle_wait_seconds=row.mapped_upstream_idle_wait_seconds,
                    mapped_edge_max_p90_idle_wait_seconds=row.mapped_edge_max_p90_idle_wait_seconds,
                    mapped_edge_max_avg_ready_seconds=row.mapped_edge_max_avg_ready_seconds,
                    mapped_edge_max_median_clipped_ready_seconds=row.mapped_edge_max_median_clipped_ready_seconds,
                    mapped_edge_max_p90_ready_seconds=row.mapped_edge_max_p90_ready_seconds,
                    mapped_edge_max_avg_sensor_touch_seconds=row.mapped_edge_max_avg_sensor_touch_seconds,
                    mapped_edge_max_p90_sensor_touch_seconds=row.mapped_edge_max_p90_sensor_touch_seconds,
                    strategy=solve_result.rejection_reason or "solver_rejected",
                    recent_observed_effective_start_utc=format_minute_of_day(
                        recent_observed_effective_start_minutes.get(row.dag_id, current_effective_start_minute)
                    ),
                )
            )

    for assignment in slotted_assignments:
        row = rows_by_dag_id[assignment.dag_id]
        proposed_primary_start_minute = assignment.proposed_primary_start_minute
        proposed_effective_start_minute = assignment.proposed_effective_start_minute

        proposed_minute = proposed_primary_start_minute % 60
        proposed_hour = proposed_primary_start_minute // 60
        proposed_schedule = format_cron(proposed_minute, [proposed_hour], row.schedule_suffix)

        if row.dependency_gate_offset_minutes > 0:
            current_dependency_gate_minute = row.current_effective_start_minute + row.dependency_gate_offset_minutes
            proposed_dependency_gate_minute = proposed_effective_start_minute + row.dependency_gate_offset_minutes
            current_wait_before_ready_minutes = max(0, row.upstream_ready_minute - current_dependency_gate_minute)
            proposed_wait_before_ready_minutes = max(0, row.upstream_ready_minute - proposed_dependency_gate_minute)
            current_gap_after_ready_minutes = max(0, current_dependency_gate_minute - row.upstream_ready_minute)
            proposed_gap_after_ready_minutes = max(0, proposed_dependency_gate_minute - row.upstream_ready_minute)
        else:
            current_wait_before_ready_minutes = max(0, row.upstream_ready_minute - row.current_primary_start_minute)
            proposed_wait_before_ready_minutes = max(0, row.upstream_ready_minute - proposed_primary_start_minute)
            current_gap_after_ready_minutes = max(0, row.current_primary_start_minute - row.upstream_ready_minute)
            proposed_gap_after_ready_minutes = max(0, proposed_primary_start_minute - row.upstream_ready_minute)
            current_effective_start_minute = max(row.current_primary_start_minute, row.upstream_ready_minute) + row.post_ready_setup_minutes
            proposed_effective_start_minute = max(proposed_primary_start_minute, row.upstream_ready_minute) + row.post_ready_setup_minutes

        proposal_rows.append(
            ProposalRow(
                dag_id=row.dag_id,
                current_schedule=row.current_schedule,
                proposed_schedule=proposed_schedule,
                current_primary_start_utc=format_minute_of_day(row.current_primary_start_minute),
                proposed_primary_start_utc=format_minute_of_day(proposed_primary_start_minute),
                current_effective_start_utc=format_minute_of_day(current_effective_start_minute),
                proposed_effective_start_utc=format_minute_of_day(proposed_effective_start_minute),
                estimated_upstream_ready_utc=format_minute_of_day(row.upstream_ready_minute),
                current_wait_before_ready_minutes=current_wait_before_ready_minutes,
                proposed_wait_before_ready_minutes=proposed_wait_before_ready_minutes,
                current_gap_after_ready_minutes=current_gap_after_ready_minutes,
                proposed_gap_after_ready_minutes=proposed_gap_after_ready_minutes,
                wait_saved_minutes=current_wait_before_ready_minutes - proposed_wait_before_ready_minutes,
                current_estimated_finish_utc=format_minute_of_day(add_minutes(row.current_primary_start_minute, int(round(row.median_schedule_to_end_seconds / 60.0)) if row.median_schedule_to_end_seconds else row.typical_processing_minutes)),
                proposed_estimated_finish_utc=format_minute_of_day(add_minutes(proposed_primary_start_minute, int(round(row.median_schedule_to_end_seconds / 60.0)) if row.median_schedule_to_end_seconds else row.typical_processing_minutes)),
                shift_minutes=proposed_primary_start_minute - row.current_primary_start_minute,
                pressure_buffer_minutes=row.pressure_buffer_minutes,
                effective_start_delay_minutes=row.effective_start_delay_minutes,
                post_ready_setup_minutes=row.post_ready_setup_minutes,
                direct_upstream_dependency_count=row.direct_upstream_dependency_count,
                avg_dag_runtime_seconds=row.avg_dag_runtime_seconds,
                median_dag_runtime_seconds=row.median_dag_runtime_seconds,
                p90_dag_runtime_seconds=row.p90_dag_runtime_seconds,
                avg_effective_start_delay_seconds=row.avg_effective_start_delay_seconds,
                p90_effective_start_delay_seconds=row.p90_effective_start_delay_seconds,
                avg_effective_processing_seconds=row.avg_effective_processing_seconds,
                median_effective_processing_seconds=row.median_effective_processing_seconds,
                p90_effective_processing_seconds=row.p90_effective_processing_seconds,
                total_scoped_idle_wait_seconds=row.total_scoped_idle_wait_seconds,
                mapped_upstream_idle_wait_seconds=row.mapped_upstream_idle_wait_seconds,
                mapped_edge_max_p90_idle_wait_seconds=row.mapped_edge_max_p90_idle_wait_seconds,
                mapped_edge_max_avg_ready_seconds=row.mapped_edge_max_avg_ready_seconds,
                mapped_edge_max_median_clipped_ready_seconds=row.mapped_edge_max_median_clipped_ready_seconds,
                mapped_edge_max_p90_ready_seconds=row.mapped_edge_max_p90_ready_seconds,
                mapped_edge_max_avg_sensor_touch_seconds=row.mapped_edge_max_avg_sensor_touch_seconds,
                mapped_edge_max_p90_sensor_touch_seconds=row.mapped_edge_max_p90_sensor_touch_seconds,
                strategy=assignment.strategy,
                recent_observed_effective_start_utc=format_minute_of_day(
                    recent_observed_effective_start_minutes.get(row.dag_id, current_effective_start_minute)
                ),
            )
        )

    proposal_rows.extend(fixed_rows)
    proposal_rows.sort(key=lambda item: item.mapped_upstream_idle_wait_seconds, reverse=True)

    total_wait_saved_minutes = sum(proposal.wait_saved_minutes for proposal in proposal_rows)
    rescheduled_count = sum(1 for proposal in proposal_rows if proposal.shift_minutes != 0)

    diagnostics_by_dag: dict[str, list[tuple[object, ...]]] = defaultdict(list)
    proposal_dag_ids = {proposal.dag_id for proposal in proposal_rows}
    historical_profiles_by_day: dict[str, dict[str, RepresentativeRunProfile]] = defaultdict(dict)
    representative_profiles: dict[str, RepresentativeRunProfile | None] = {}
    if not reviewed_assumptions_first:
        diagnostic_rows = connection.execute(
            f"""
            SELECT
                to_dag_id,
                from_dag_id,
                sensor_task_id,
                run_id,
                CAST(logical_date AS VARCHAR) AS logical_date,
                raw_ready_seconds,
                clipped_ready_seconds,
                ready_seconds_was_clipped
            FROM {scope.view_name('seed_edge_wait_runs')}
            WHERE run_id IS NOT NULL
            ORDER BY to_dag_id, from_dag_id, logical_date
            """
        ).fetchall()
        for diagnostic_row in diagnostic_rows:
            dag_id = str(diagnostic_row[0])
            if dag_id in proposal_dag_ids:
                diagnostics_by_dag[dag_id].append(diagnostic_row)

        representative_run_rows = connection.execute(
            f"""
            WITH create_config AS (
                SELECT
                    dag_id,
                    run_id,
                    start_date AS create_config_start,
                    EXTRACT(EPOCH FROM (start_date - scheduled_at)) AS create_config_delay_seconds
                FROM task_instances_enriched
                WHERE task_id IN ('create_config', 'create_run_config')
            )
            SELECT
                dr.dag_id,
                dr.run_id,
                CAST(dr.logical_date AS VARCHAR) AS logical_date,
                dr.start_delay_seconds,
                dr.dag_runtime_seconds,
                dr.schedule_to_end_seconds,
                cc.create_config_delay_seconds,
                CASE
                    WHEN cc.create_config_start IS NOT NULL THEN EXTRACT(EPOCH FROM (dr.end_date - cc.create_config_start))
                    ELSE NULL
                END AS create_config_to_end_seconds
            FROM dag_runs_enriched dr
            LEFT JOIN create_config cc
              ON cc.dag_id = dr.dag_id
             AND cc.run_id = dr.run_id
            WHERE dr.state = 'success'
              AND dr.end_date IS NOT NULL
              AND dr.dag_id IN ({", ".join(repr(dag_id) for dag_id in sorted(proposal_dag_ids))})
            ORDER BY dr.dag_id, dr.logical_date
            """
        ).fetchall()
        representative_runs_by_dag: dict[str, list[RepresentativeRunRow]] = defaultdict(list)
        for run_row in representative_run_rows:
            representative_runs_by_dag[str(run_row[0])].append(
                RepresentativeRunRow(
                    dag_id=str(run_row[0]),
                    run_id=str(run_row[1]),
                    logical_date=str(run_row[2]),
                    start_delay_seconds=_coerce_float(run_row[3]),
                    dag_runtime_seconds=_coerce_float(run_row[4]),
                    schedule_to_end_seconds=_coerce_float(run_row[5]),
                    create_config_delay_seconds=_coerce_float(run_row[6]),
                    create_config_to_end_seconds=_coerce_float(run_row[7]),
                )
            )
        representative_profiles = {
            dag_id: _choose_representative_run(run_rows)
            for dag_id, run_rows in representative_runs_by_dag.items()
        }
        for dag_id, run_rows in representative_runs_by_dag.items():
            for run_row in run_rows:
                profile = build_replay_profile(run_row)
                if profile is None:
                    continue
                logical_dt = _coerce_datetime(profile.logical_date)
                day_key = logical_dt.date().isoformat() if logical_dt is not None else str(profile.logical_date).split()[0]
                existing_profile = historical_profiles_by_day[day_key].get(dag_id)
                if existing_profile is None or str(profile.logical_date) < str(existing_profile.logical_date):
                    historical_profiles_by_day[day_key][dag_id] = profile
    task_sum_estimates = load_task_sum_estimates(connection, proposal_dag_ids, runtime_estimation_config)
    hourly_pressure_csv_path = ARTIFACTS_DIR / f"{scope.artifact_prefix}_hourly_pressure_parallel.csv"
    observed_global_limits_csv_path = ARTIFACTS_DIR / f"{scope.artifact_prefix}_observed_global_limits.csv"
    observed_per_dag_limits_csv_path = ARTIFACTS_DIR / f"{scope.artifact_prefix}_observed_per_dag_limits.csv"
    observed_global_task_peak = load_observed_global_task_peak(connection)
    observed_scoped_task_peak = load_observed_scoped_task_peak(connection, proposal_dag_ids, scope.scope_id)
    observed_per_dag_run_peaks = load_observed_per_dag_run_peaks(connection, proposal_dag_ids)
    observed_scoped_peak_profile = load_observed_task_peak_profile(connection, bucket_minutes, proposal_dag_ids)
    replay_profiles = [
        profile
        for profiles_by_dag in historical_profiles_by_day.values()
        for profile in profiles_by_dag.values()
    ]
    if not replay_profiles:
        replay_profiles = [profile for profile in representative_profiles.values() if profile is not None]
        if replay_profiles:
            historical_profiles_by_day = {
                "representative": {profile.dag_id: profile for profile in replay_profiles}
            }
    replay_task_intervals = load_task_intervals_for_profiles(connection, replay_profiles) if replay_profiles else {}
    all_task_intervals_by_dag = load_task_intervals_by_dag(connection)
    scoped_task_intervals_by_dag = {
        dag_id: intervals
        for dag_id, intervals in all_task_intervals_by_dag.items()
        if dag_id in proposal_dag_ids
    }

    with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(proposal_rows[0].to_dict().keys()))
        writer.writeheader()
        writer.writerows(proposal.to_dict() for proposal in proposal_rows)

    reviewed_assumption_rows.sort(
        key=lambda row: (
            0 if row["confidence"] == "hard_fact" else 1 if row["confidence"] == "reviewed_assumption" else 2,
            str(row["dag_id"]),
        )
    )
    with reviewed_assumptions_csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(reviewed_assumption_rows[0].keys()))
        writer.writeheader()
        writer.writerows(reviewed_assumption_rows)
    reviewed_assumptions_markdown_path.write_text(
        render_reviewed_assumptions_markdown(
            scope_display_name=scope.display_name,
            solver_backend=solver_config.backend,
            solver_objective_mode=solver_config.objective_mode,
            reviewed_assumption_rows=reviewed_assumption_rows,
        ),
        encoding="utf-8",
    )

    observed_global_rows = [
        {
            "metric": "global_running_tasks",
            "subject": "all_dags",
            "reference_limit_name": "parallelism",
            "reference_limit_value": 24,
            "observed_peak": observed_global_task_peak.observed_peak,
            "peak_time": observed_global_task_peak.peak_time,
            "within_limit": observed_global_task_peak.observed_peak <= 24,
            "limit_headroom": 24 - observed_global_task_peak.observed_peak,
            "limit_status": _limit_status(observed_global_task_peak.observed_peak, 24),
        },
        {
            "metric": "scoped_running_tasks",
            "subject": scope.scope_id,
            "reference_limit_name": "parallelism",
            "reference_limit_value": 24,
            "observed_peak": observed_scoped_task_peak.observed_peak,
            "peak_time": observed_scoped_task_peak.peak_time,
            "within_limit": observed_scoped_task_peak.observed_peak <= 24,
            "limit_headroom": 24 - observed_scoped_task_peak.observed_peak,
            "limit_status": _limit_status(observed_scoped_task_peak.observed_peak, 24),
        },
    ]
    with observed_global_limits_csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(observed_global_rows[0].keys()))
        writer.writeheader()
        writer.writerows(observed_global_rows)

    observed_per_dag_rows = []
    for dag_id in sorted(proposal_dag_ids):
        task_peak = observed_per_dag_task_peaks.get(dag_id, ObservedPeak(subject=dag_id, observed_peak=0, peak_time=""))
        run_peak = observed_per_dag_run_peaks.get(dag_id, ObservedPeak(subject=dag_id, observed_peak=0, peak_time=""))
        observed_per_dag_rows.append(
            {
                "dag_id": dag_id,
                "configured_max_active_tasks_per_dag": 8,
                "observed_peak_running_tasks": task_peak.observed_peak,
                "running_tasks_peak_time": task_peak.peak_time,
                "within_max_active_tasks_per_dag": task_peak.observed_peak <= 8,
                "task_limit_headroom": 8 - task_peak.observed_peak,
                "task_limit_status": _limit_status(task_peak.observed_peak, 8),
                "configured_max_active_runs_per_dag": 1,
                "observed_peak_active_runs": run_peak.observed_peak,
                "active_runs_peak_time": run_peak.peak_time,
                "within_max_active_runs_per_dag": run_peak.observed_peak <= 1,
                "run_limit_headroom": 1 - run_peak.observed_peak,
                "run_limit_status": _limit_status(run_peak.observed_peak, 1),
            }
        )
    with observed_per_dag_limits_csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(observed_per_dag_rows[0].keys()))
        writer.writeheader()
        writer.writerows(observed_per_dag_rows)

    current_scoped_parallel_tasks, proposed_scoped_parallel_tasks = _build_scoped_parallel_task_series(
        proposal_rows,
        representative_profiles,
        runtime_estimation_config,
        task_sum_estimates,
        bucket_minutes,
    )
    current_scoped_peak_tasks, proposed_scoped_peak_tasks = _build_scoped_peak_task_series(
        proposal_rows,
        historical_profiles_by_day,
        replay_task_intervals,
        bucket_minutes,
    )
    (
        current_scoped_peak_tasks_shifted_exact,
        proposed_scoped_peak_tasks_shifted_exact,
        current_global_peak_profile_shifted_exact,
        proposed_global_peak_profile_shifted_exact,
    ) = _build_exact_shifted_peak_task_series(
        proposal_rows,
        scoped_task_intervals_by_dag,
        all_task_intervals_by_dag,
        bucket_minutes,
    )
    estimated_proposed_global_pressure = {
        minute_of_day: max(
            0.0,
            global_pressure_by_minute.get(minute_of_day, 0.0)
            - current_scoped_parallel_tasks.get(minute_of_day, 0.0)
            + proposed_scoped_parallel_tasks.get(minute_of_day, 0.0),
        )
        for minute_of_day in range(0, 24 * 60, bucket_minutes)
    }
    estimated_proposed_global_peak_profile = {
        minute_of_day: max(0.0, observed_non_scoped_peak_profile.get(minute_of_day, 0.0))
        + proposed_scoped_peak_tasks.get(minute_of_day, 0.0)
        for minute_of_day in range(0, 24 * 60, bucket_minutes)
    }
    estimated_current_global_peak_profile = {
        minute_of_day: max(0.0, observed_non_scoped_peak_profile.get(minute_of_day, 0.0))
        + current_scoped_peak_tasks.get(minute_of_day, 0.0)
        for minute_of_day in range(0, 24 * 60, bucket_minutes)
    }
    current_global_pressure_hourly = _hourly_average_series(global_pressure_by_minute, bucket_minutes)
    proposed_global_pressure_hourly = _hourly_average_series(estimated_proposed_global_pressure, bucket_minutes)
    current_global_parallel_tasks_hourly = _hourly_peak_slot_series(observed_global_peak_profile, bucket_minutes)
    proposed_global_parallel_tasks_hourly = _hourly_peak_slot_series(estimated_proposed_global_peak_profile, bucket_minutes)
    current_global_parallel_tasks_hourly_estimated = _hourly_peak_slot_series(estimated_current_global_peak_profile, bucket_minutes)
    current_global_parallel_tasks_hourly_shifted_exact = _hourly_peak_slot_series(
        current_global_peak_profile_shifted_exact,
        bucket_minutes,
    )
    proposed_global_parallel_tasks_hourly_shifted_exact = _hourly_peak_slot_series(
        proposed_global_peak_profile_shifted_exact,
        bucket_minutes,
    )
    current_ds_pressure_hourly = _hourly_average_series(current_scoped_parallel_tasks, bucket_minutes)
    proposed_ds_pressure_hourly = _hourly_average_series(proposed_scoped_parallel_tasks, bucket_minutes)
    current_ds_parallel_tasks_hourly = _hourly_peak_slot_series(observed_scoped_peak_profile, bucket_minutes)
    proposed_ds_parallel_tasks_hourly = _hourly_peak_slot_series(proposed_scoped_peak_tasks, bucket_minutes)
    current_ds_parallel_tasks_hourly_estimated = _hourly_peak_slot_series(current_scoped_peak_tasks, bucket_minutes)
    current_ds_parallel_tasks_hourly_shifted_exact = _hourly_peak_slot_series(
        current_scoped_peak_tasks_shifted_exact,
        bucket_minutes,
    )
    proposed_ds_parallel_tasks_hourly_shifted_exact = _hourly_peak_slot_series(
        proposed_scoped_peak_tasks_shifted_exact,
        bucket_minutes,
    )
    hourly_pressure_rows = [
        {
            "hour": f"{hour:02d}:00",
            "global_avg_concurrency_current": current_global_pressure_hourly[hour],
            "global_avg_concurrency_proposed": proposed_global_pressure_hourly[hour],
            "global_peak_parallel_tasks_current": current_global_parallel_tasks_hourly[hour],
            "global_peak_parallel_tasks_current_estimated": current_global_parallel_tasks_hourly_estimated[hour],
            "global_peak_parallel_tasks_proposed": proposed_global_parallel_tasks_hourly[hour],
            "global_peak_parallel_tasks_current_shifted_exact": current_global_parallel_tasks_hourly_shifted_exact[hour],
            "global_peak_parallel_tasks_proposed_shifted_exact": proposed_global_parallel_tasks_hourly_shifted_exact[hour],
            "ds_avg_concurrency_current": current_ds_pressure_hourly[hour],
            "ds_avg_concurrency_proposed": proposed_ds_pressure_hourly[hour],
            "ds_peak_parallel_tasks_current": current_ds_parallel_tasks_hourly[hour],
            "ds_peak_parallel_tasks_current_estimated": current_ds_parallel_tasks_hourly_estimated[hour],
            "ds_peak_parallel_tasks_proposed": proposed_ds_parallel_tasks_hourly[hour],
            "ds_peak_parallel_tasks_current_shifted_exact": current_ds_parallel_tasks_hourly_shifted_exact[hour],
            "ds_peak_parallel_tasks_proposed_shifted_exact": proposed_ds_parallel_tasks_hourly_shifted_exact[hour],
        }
        for hour in range(24)
    ]
    with hourly_pressure_csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(hourly_pressure_rows[0].keys()))
        writer.writeheader()
        writer.writerows(hourly_pressure_rows)
    mermaid_chart_path.write_text(
        _build_combined_hourly_xychart(
            f"{scope.display_name} Global Airflow Load by UTC Hour",
            "UTC time",
            [f"{hour:02d}:00" for hour in range(24)],
            current_global_pressure_hourly,
            proposed_global_pressure_hourly,
            current_global_parallel_tasks_hourly,
            proposed_global_parallel_tasks_hourly,
        ),
        encoding="utf-8",
    )
    global_mermaid_chart_path.write_text(
        _build_global_pressure_xychart(
            f"{scope.display_name} Global Pressure by UTC Hour",
            "UTC time",
            [f"{hour:02d}:00" for hour in range(24)],
            current_global_pressure_hourly,
            proposed_global_pressure_hourly,
        ),
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_schedule_proposal_markdown(
            scope_display_name=scope.display_name,
            scope_id=scope.scope_id,
            solver_backend=solver_config.backend,
            solver_objective_mode=solver_config.objective_mode,
            sequential_dag_pairs=solver_config.sequential_dag_pairs,
            solver_status=solve_result.status,
            solver_rejection_reason=solve_result.rejection_reason,
            proposal_rows=proposal_rows,
            working_hours=working_hours,
            bucket_minutes=bucket_minutes,
            min_gap_minutes=min_gap_minutes,
            rescheduled_count=rescheduled_count,
            total_wait_saved_minutes=total_wait_saved_minutes,
            reviewed_assumptions_csv_name=reviewed_assumptions_csv_path.name,
            reviewed_assumptions_markdown_name=reviewed_assumptions_markdown_path.name,
            reviewed_assumption_rows=reviewed_assumption_rows,
            include_runtime_diagnostics=not reviewed_assumptions_first,
            observed_global_limits_csv_name=observed_global_limits_csv_path.name,
            observed_per_dag_limits_csv_name=observed_per_dag_limits_csv_path.name,
            representative_profiles=representative_profiles,
            runtime_estimation_config=runtime_estimation_config,
            diagnostics_by_dag=diagnostics_by_dag,
            task_sum_estimates=task_sum_estimates,
            task_count_estimates=task_count_estimates,
            global_pressure_by_minute=global_pressure_by_minute,
            current_global_pressure_hourly=current_global_pressure_hourly,
            proposed_global_pressure_hourly=proposed_global_pressure_hourly,
            current_ds_pressure_hourly=current_ds_pressure_hourly,
            proposed_ds_pressure_hourly=proposed_ds_pressure_hourly,
            hourly_pressure_csv_name=hourly_pressure_csv_path.name,
            mermaid_chart_name=mermaid_chart_path.name,
            global_mermaid_chart_name=global_mermaid_chart_path.name,
            append_hourly_table=_append_hourly_table,
            append_hourly_delta_summary=_append_hourly_delta_summary,
        ),
        encoding="utf-8",
    )
    print(f"wrote {markdown_path}")
    print(f"wrote {csv_path}")
    print(f"wrote {reviewed_assumptions_csv_path}")
    print(f"wrote {reviewed_assumptions_markdown_path}")
    print(f"wrote {hourly_pressure_csv_path}")
    print(f"wrote {observed_global_limits_csv_path}")
    print(f"wrote {observed_per_dag_limits_csv_path}")
    return markdown_path


def build_recommendation_engine_schedule_proposal(
    connection: duckdb.DuckDBPyConnection,
    solver_backend: str | None = None,
    solver_objective_mode: str | None = None,
) -> Path:
    return build_scope_schedule_proposal(
        connection,
        get_scope("recommendation_engine"),
        solver_backend=solver_backend,
        solver_objective_mode=solver_objective_mode,
    )