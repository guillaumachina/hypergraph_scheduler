from __future__ import annotations

import math

from hypergraph_scheduler.scheduling.models import GlobalPressureProfile, SlottedDagPlanInput, WorkingHours
from hypergraph_scheduler.scheduling.time_utils import round_down_to_bucket


def iter_candidate_slots(working_hours: WorkingHours, bucket_minutes: int) -> list[int]:
    return list(range(working_hours.earliest_start_minute, working_hours.latest_start_minute + 1, bucket_minutes))


def iter_window_buckets(window_start_minute: int, window_end_minute: int, bucket_minutes: int) -> range:
    bucket_start = round_down_to_bucket(window_start_minute, bucket_minutes)
    bucket_end = max(
        bucket_start,
        round_down_to_bucket(max(window_end_minute - 1, window_start_minute), bucket_minutes),
    )
    return range(bucket_start, bucket_end + bucket_minutes, bucket_minutes)


def slotted_row_sort_key(item: SlottedDagPlanInput) -> tuple[int, float, str]:
    return (
        item.current_effective_start_minute,
        -item.mapped_upstream_idle_wait_seconds,
        item.dag_id,
    )


def task_load_weight(task_count: float | None) -> float:
    if task_count is None or task_count <= 0:
        return 1.0
    return math.sqrt(task_count)


def average_global_pressure_for_window(
    pressure_by_minute: GlobalPressureProfile | None,
    window_start_minute: int,
    window_end_minute: int,
    bucket_minutes: int,
) -> float:
    if not pressure_by_minute:
        return 0.0

    bucket_start = round_down_to_bucket(window_start_minute, bucket_minutes)
    bucket_end = max(bucket_start, round_down_to_bucket(max(window_end_minute - 1, window_start_minute), bucket_minutes))
    bucket_values = [
        pressure_by_minute.get(minute_of_day % (24 * 60), 0.0)
        for minute_of_day in range(bucket_start, bucket_end + bucket_minutes, bucket_minutes)
    ]
    if not bucket_values:
        return 0.0
    return sum(bucket_values) / len(bucket_values)


def choose_primary_start_slot(
    *,
    current_primary_start_minute: int,
    current_effective_start_minute: int | None = None,
    assigned_effective_starts: list[int],
    assigned_load_windows: list[tuple[int, int, float] | tuple[int, int, float, float] | tuple[int, int, float, float, GlobalPressureProfile]] | None = None,
    global_pressure_by_minute: GlobalPressureProfile | None = None,
    global_peak_by_minute: GlobalPressureProfile | None = None,
    working_hours: WorkingHours,
    bucket_minutes: int,
    min_gap_minutes: int,
    finish_deadline_minute: int,
    effective_processing_minutes: int,
    upstream_ready_minute: int,
    dependency_gate_offset_minutes: int = 0,
    post_ready_setup_minutes: int,
    task_load_weight: float = 1.0,
    task_peak_estimate: float = 0.0,
    task_peak_profile_by_minute: GlobalPressureProfile | None = None,
    observed_global_peak_target_by_minute: GlobalPressureProfile | None = None,
    background_effective_starts_by_minute: GlobalPressureProfile | None = None,
    observed_global_effective_start_target_by_minute: GlobalPressureProfile | None = None,
    objective_mode: str = "wait_saving",
    parallelism_limit: int | None = None,
    force_earliest_ready_slot: bool = False,
    minimum_primary_start_minute: int | None = None,
) -> tuple[int, int]:
    candidate_slots = iter_candidate_slots(working_hours, bucket_minutes)
    if minimum_primary_start_minute is not None:
        minimum_candidate_slots = [slot for slot in candidate_slots if slot >= minimum_primary_start_minute]
        candidate_slots = minimum_candidate_slots if minimum_candidate_slots else candidate_slots[-1:]
    if force_earliest_ready_slot:
        ready_candidate_slots = [slot for slot in candidate_slots if slot >= upstream_ready_minute]
        candidate_slots = ready_candidate_slots[:1] if ready_candidate_slots else candidate_slots[-1:]
    best_primary_slot = candidate_slots[0]
    effective_shift_anchor = current_effective_start_minute if current_effective_start_minute is not None else current_primary_start_minute
    observed_global_peak_cap = (
        max(observed_global_peak_target_by_minute.values()) if observed_global_peak_target_by_minute else None
    )
    observed_global_effective_start_cap = (
        max(observed_global_effective_start_target_by_minute.values())
        if observed_global_effective_start_target_by_minute
        else None
    )
    if dependency_gate_offset_minutes > 0:
        best_effective_slot = best_primary_slot
    else:
        best_effective_slot = max(best_primary_slot, upstream_ready_minute) + post_ready_setup_minutes
    best_score: float | None = None

    for slot in candidate_slots:
        if dependency_gate_offset_minutes > 0:
            predicted_effective_start = slot
        else:
            predicted_effective_start = max(slot, upstream_ready_minute) + post_ready_setup_minutes
        predicted_dependency_gate = predicted_effective_start + dependency_gate_offset_minutes
        nearest_gap = min((abs(predicted_effective_start - assigned) for assigned in assigned_effective_starts), default=min_gap_minutes)
        gap_violation = max(0, min_gap_minutes - nearest_gap)
        if dependency_gate_offset_minutes > 0:
            wait_before_ready = max(0, upstream_ready_minute - predicted_dependency_gate)
            late_after_ready = max(0, predicted_dependency_gate - upstream_ready_minute)
        else:
            wait_before_ready = max(0, upstream_ready_minute - slot)
            late_after_ready = max(0, slot - upstream_ready_minute)
        schedule_shift = abs(slot - current_primary_start_minute)
        finish_overrun = max(0, predicted_effective_start + effective_processing_minutes - finish_deadline_minute)
        overlap_load_penalty = 0.0
        predicted_finish = predicted_effective_start + effective_processing_minutes
        normalized_assigned_windows: list[tuple[int, int, float, float, GlobalPressureProfile | None]] = []
        for assigned_window in assigned_load_windows or []:
            if len(assigned_window) == 3:
                assigned_start, assigned_finish, assigned_weight = assigned_window
                assigned_peak_estimate = float(assigned_weight)
                assigned_peak_profile = None
            elif len(assigned_window) == 4:
                assigned_start, assigned_finish, assigned_weight, assigned_peak_estimate = assigned_window
                assigned_peak_profile = None
            else:
                assigned_start, assigned_finish, assigned_weight, assigned_peak_estimate, assigned_peak_profile = assigned_window
            normalized_assigned_windows.append(
                (assigned_start, assigned_finish, assigned_weight, float(assigned_peak_estimate), assigned_peak_profile)
            )

        for assigned_start, assigned_finish, assigned_weight, _, _ in normalized_assigned_windows:
            overlap_minutes = max(0, min(predicted_finish, assigned_finish) - max(predicted_effective_start, assigned_start))
            if overlap_minutes > 0:
                overlap_load_penalty += overlap_minutes * assigned_weight
        average_global_pressure = average_global_pressure_for_window(
            global_pressure_by_minute,
            predicted_effective_start,
            predicted_finish,
            bucket_minutes,
        )
        projected_global_peak = 0.0
        projected_global_target_excess_penalty = 0.0
        if global_peak_by_minute is not None and (
            parallelism_limit is not None or observed_global_peak_target_by_minute is not None
        ):
            shift_minutes = predicted_effective_start - effective_shift_anchor
            shifted_task_peak_profile = None
            if task_peak_profile_by_minute:
                shifted_task_peak_profile = {
                    (minute_of_day + shift_minutes) % (24 * 60): peak_value
                    for minute_of_day, peak_value in task_peak_profile_by_minute.items()
                }
            if shifted_task_peak_profile:
                bucket_domain = set(global_peak_by_minute)
                bucket_domain.update(shifted_task_peak_profile)
                for _, _, _, _, assigned_peak_profile in normalized_assigned_windows:
                    if assigned_peak_profile:
                        bucket_domain.update(assigned_peak_profile)
                for minute_of_day in bucket_domain:
                    overlapping_peak = shifted_task_peak_profile.get(minute_of_day, 0.0)
                    for _, _, _, assigned_peak_estimate, assigned_peak_profile in normalized_assigned_windows:
                        if assigned_peak_profile is not None:
                            overlapping_peak += assigned_peak_profile.get(minute_of_day, 0.0)
                        else:
                            overlapping_peak += assigned_peak_estimate
                    projected_bucket_load = global_peak_by_minute.get(minute_of_day, 0.0) + overlapping_peak
                    projected_global_peak = max(projected_global_peak, projected_bucket_load)
                    if observed_global_peak_cap is not None:
                        target_excess = max(
                            0.0,
                            projected_bucket_load - observed_global_peak_cap,
                        )
                        projected_global_target_excess_penalty += target_excess * target_excess
            else:
                for bucket_minute in iter_window_buckets(predicted_effective_start, predicted_finish, bucket_minutes):
                    minute_of_day = bucket_minute % (24 * 60)
                    bucket_end = bucket_minute + bucket_minutes
                    overlapping_peak = task_peak_estimate
                    for assigned_start, assigned_finish, _, assigned_peak_estimate, _ in normalized_assigned_windows:
                        if assigned_start < bucket_end and assigned_finish > bucket_minute:
                            overlapping_peak += assigned_peak_estimate
                    projected_bucket_load = global_peak_by_minute.get(minute_of_day, 0.0) + overlapping_peak
                    projected_global_peak = max(projected_global_peak, projected_bucket_load)
                    if observed_global_peak_cap is not None:
                        target_excess = max(
                            0.0,
                            projected_bucket_load - observed_global_peak_cap,
                        )
                        projected_global_target_excess_penalty += target_excess * target_excess
        peak_soft_excess = 0.0
        peak_hard_excess = 0.0
        if parallelism_limit is not None:
            peak_soft_excess = max(0.0, projected_global_peak - 0.75 * parallelism_limit)
            peak_hard_excess = max(0.0, projected_global_peak - parallelism_limit)

        start_target_excess_penalty = 0.0
        if observed_global_effective_start_cap is not None:
            start_bucket_minute = round_down_to_bucket(predicted_effective_start, bucket_minutes) % (24 * 60)
            assigned_start_count = sum(
                1
                for assigned in assigned_effective_starts
                if round_down_to_bucket(assigned, bucket_minutes) % (24 * 60) == start_bucket_minute
            )
            projected_effective_start_count = (
                (background_effective_starts_by_minute or {}).get(start_bucket_minute, 0.0)
                + assigned_start_count
                + 1.0
            )
            start_target_excess = max(
                0.0,
                projected_effective_start_count - observed_global_effective_start_cap,
            )
            start_target_excess_penalty = start_target_excess * start_target_excess

        if objective_mode == "concurrency_first":
            score = (
                500.0 * projected_global_target_excess_penalty
                + 500.0 * start_target_excess_penalty
                + 25.0 * peak_hard_excess * peak_hard_excess
                + 2.0 * peak_soft_excess * peak_soft_excess
                + 0.5 * task_load_weight * average_global_pressure
                + 0.05 * schedule_shift
                + 0.25 * wait_before_ready
                + 0.1 * late_after_ready
                + 2.0 * gap_violation
                + 2.0 * finish_overrun
                + 0.01 * task_load_weight * overlap_load_penalty
            )
        else:
            score = (
                10.0 * gap_violation
                + 6.0 * finish_overrun
                + 5.0 * wait_before_ready
                + 1.25 * late_after_ready
                + 0.1 * schedule_shift
                + 0.01 * task_load_weight * overlap_load_penalty
                + 0.5 * task_load_weight * average_global_pressure
                + 500.0 * projected_global_target_excess_penalty
                + 200.0 * start_target_excess_penalty
                + 2.0 * peak_soft_excess * peak_soft_excess
                + 25.0 * peak_hard_excess * peak_hard_excess
            )

        if best_score is None or score < best_score:
            best_score = score
            best_primary_slot = slot
            best_effective_slot = predicted_effective_start

    return best_primary_slot, best_effective_slot
