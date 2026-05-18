from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from math import ceil

from hypergraph_scheduler.scheduling.models import (
    GlobalPressureProfile,
    ObservedPeak,
    SchedulingSolveResult,
    SchedulingSolverConfig,
    SlottedDagAssignment,
    SlottedDagPlanInput,
    WorkingHours,
)
from hypergraph_scheduler.scheduling.slot_optimization import (
    average_global_pressure_for_window,
    choose_primary_start_slot,
    iter_candidate_slots,
    iter_window_buckets,
    slotted_row_sort_key,
    task_load_weight,
)
from hypergraph_scheduler.scheduling.time_utils import round_down_to_bucket


BASE_COST_SCALE = 100
PEAK_BOUND_WEIGHT = 100_000
CONCURRENCY_FIRST_PEAK_BOUND_WEIGHT = 10_000
OBSERVED_PROFILE_EXCESS_WEIGHT = 1_000_000
SOFT_EXCESS_WEIGHT = 500
HARD_EXCESS_WEIGHT = 5_000
PAIR_GAP_WEIGHT = 1_000
PAIR_OVERLAP_WEIGHT = 1
HEAVY_PAIR_SEQUENCE_WEIGHT = 2_000


@dataclass(frozen=True)
class _SlotCandidate:
    row_index: int
    slot_index: int
    primary_start_minute: int
    effective_start_minute: int
    finish_minute: int
    base_cost: int
    peak_contribution: int
    effective_start_bucket_minute: int
    bucket_minutes: tuple[int, ...]
    peak_profile_by_minute: tuple[tuple[int, int], ...]


def _shift_peak_profile(
    profile_by_minute: GlobalPressureProfile | None,
    shift_minutes: int,
) -> dict[int, int]:
    if not profile_by_minute:
        return {}
    shifted: dict[int, int] = {}
    for minute_of_day, peak_value in profile_by_minute.items():
        shifted[(minute_of_day + shift_minutes) % (24 * 60)] = int(round(peak_value))
    return shifted


def _predict_effective_start(row: SlottedDagPlanInput, slot: int) -> int:
    if row.dependency_gate_offset_minutes > 0:
        return slot
    return max(slot, row.upstream_ready_minute) + row.post_ready_setup_minutes


def _background_gap_penalty(effective_start: int, assigned_effective_starts: list[int], min_gap_minutes: int) -> int:
    return sum(max(0, min_gap_minutes - abs(effective_start - assigned)) for assigned in assigned_effective_starts)


def _background_overlap_penalty(
    effective_start: int,
    finish_minute: int,
    assigned_load_windows: list[tuple[int, int, float, float] | tuple[int, int, float, float, GlobalPressureProfile]],
) -> float:
    penalty = 0.0
    for assigned_window in assigned_load_windows:
        assigned_start, assigned_finish, assigned_weight = assigned_window[:3]
        overlap_minutes = max(0, min(finish_minute, assigned_finish) - max(effective_start, assigned_start))
        if overlap_minutes > 0:
            penalty += overlap_minutes * assigned_weight
    return penalty


def _build_candidates(
    rows: list[SlottedDagPlanInput],
    *,
    objective_mode: str,
    working_hours: WorkingHours,
    bucket_minutes: int,
    min_gap_minutes: int,
    finish_deadline_minute: int,
    assigned_effective_starts: list[int],
    assigned_load_windows: list[tuple[int, int, float, float]],
    global_pressure_by_minute: GlobalPressureProfile | None,
    observed_per_dag_task_peaks: dict[str, ObservedPeak],
    observed_per_dag_task_peak_profiles: dict[str, GlobalPressureProfile],
) -> dict[int, list[_SlotCandidate]]:
    candidates_by_row: dict[int, list[_SlotCandidate]] = {}
    for row_index, row in enumerate(rows):
        row_candidates: list[_SlotCandidate] = []
        weight = task_load_weight(row.median_task_count)
        peak_profile_by_minute = observed_per_dag_task_peak_profiles.get(row.dag_id, {})
        peak_estimate = int(
            round(float(observed_per_dag_task_peaks.get(row.dag_id, ObservedPeak(row.dag_id, 0, "")).observed_peak))
        )
        candidate_slots = list(enumerate(iter_candidate_slots(working_hours, bucket_minutes)))
        if row.force_earliest_ready_slot:
            ready_candidate_slots = [
                (slot_index, slot)
                for slot_index, slot in candidate_slots
                if slot >= row.upstream_ready_minute
            ]
            candidate_slots = ready_candidate_slots[:1] if ready_candidate_slots else candidate_slots[-1:]
        for slot_index, slot in candidate_slots:
            effective_start = _predict_effective_start(row, slot)
            predicted_dependency_gate = effective_start + row.dependency_gate_offset_minutes
            wait_before_ready = max(0, row.upstream_ready_minute - predicted_dependency_gate)
            late_after_ready = max(0, predicted_dependency_gate - row.upstream_ready_minute)
            schedule_shift = abs(slot - row.current_primary_start_minute)
            finish_minute = effective_start + row.effective_processing_minutes
            finish_overrun = max(0, finish_minute - finish_deadline_minute)
            average_pressure = average_global_pressure_for_window(
                global_pressure_by_minute,
                effective_start,
                finish_minute,
                bucket_minutes,
            )
            gap_penalty = _background_gap_penalty(effective_start, assigned_effective_starts, min_gap_minutes)
            overlap_penalty = _background_overlap_penalty(effective_start, finish_minute, assigned_load_windows)
            if objective_mode == "concurrency_first":
                base_cost = int(
                    round(
                        (
                            6.0 * gap_penalty
                            + 4.0 * finish_overrun
                            + 2.0 * wait_before_ready
                            + 4.0 * late_after_ready
                            + 0.02 * schedule_shift
                            + 0.01 * weight * overlap_penalty
                            + 0.75 * weight * average_pressure
                        )
                        * BASE_COST_SCALE
                    )
                )
            else:
                base_cost = int(
                    round(
                        (
                            10.0 * gap_penalty
                            + 6.0 * finish_overrun
                            + 5.0 * wait_before_ready
                            + 1.25 * late_after_ready
                            + 0.1 * schedule_shift
                            + 0.01 * weight * overlap_penalty
                            + 0.5 * weight * average_pressure
                        )
                        * BASE_COST_SCALE
                    )
                )
            shifted_peak_profile = _shift_peak_profile(
                peak_profile_by_minute,
                effective_start - row.current_effective_start_minute,
            )
            row_candidates.append(
                _SlotCandidate(
                    row_index=row_index,
                    slot_index=slot_index,
                    primary_start_minute=slot,
                    effective_start_minute=effective_start,
                    finish_minute=finish_minute,
                    base_cost=base_cost,
                    peak_contribution=max(max(shifted_peak_profile.values(), default=peak_estimate), 0),
                    effective_start_bucket_minute=round_down_to_bucket(effective_start, bucket_minutes) % (24 * 60),
                    bucket_minutes=tuple(
                        bucket_minute % (24 * 60)
                        for bucket_minute in iter_window_buckets(effective_start, finish_minute, bucket_minutes)
                    ),
                    peak_profile_by_minute=tuple(sorted(shifted_peak_profile.items())),
                )
            )
        candidates_by_row[row_index] = row_candidates
    return candidates_by_row


def _pair_cost(
    left_candidate: _SlotCandidate,
    right_candidate: _SlotCandidate,
    left_weight: float,
    right_weight: float,
    min_gap_minutes: int,
) -> int:
    gap_violation = max(0, min_gap_minutes - abs(left_candidate.effective_start_minute - right_candidate.effective_start_minute))
    overlap_minutes = max(
        0,
        min(left_candidate.finish_minute, right_candidate.finish_minute)
        - max(left_candidate.effective_start_minute, right_candidate.effective_start_minute),
    )
    return int(round(PAIR_GAP_WEIGHT * gap_violation + PAIR_OVERLAP_WEIGHT * overlap_minutes * left_weight * right_weight))


def _row_complexity_score(row: SlottedDagPlanInput, observed_peak: ObservedPeak | None) -> float:
    peak = float(observed_peak.observed_peak if observed_peak is not None else 0)
    runtime_severity_seconds = max(
        row.p90_dag_runtime_seconds,
        row.median_dag_runtime_seconds,
        row.p90_effective_processing_seconds,
        row.effective_processing_minutes * 60.0,
    )
    return max(1.0, peak) * task_load_weight(row.median_task_count) * max(1.0, runtime_severity_seconds / 3600.0)


def _heavy_pair_sequence_cost(
    primary_row: SlottedDagPlanInput,
    secondary_row: SlottedDagPlanInput,
    primary_candidate: _SlotCandidate,
    secondary_candidate: _SlotCandidate,
) -> int:
    overlap_minutes = max(
        0,
        min(primary_candidate.finish_minute, secondary_candidate.finish_minute)
        - max(primary_candidate.effective_start_minute, secondary_candidate.effective_start_minute),
    )
    secondary_starts_before_primary_finishes = max(0, primary_candidate.finish_minute - secondary_candidate.effective_start_minute)
    primary_late_after_ready = max(0, primary_candidate.effective_start_minute - primary_row.upstream_ready_minute)
    secondary_wait_after_ready = max(0, secondary_candidate.effective_start_minute - secondary_row.upstream_ready_minute)
    return int(
        round(
            HEAVY_PAIR_SEQUENCE_WEIGHT * secondary_starts_before_primary_finishes
            + 10.0 * HEAVY_PAIR_SEQUENCE_WEIGHT * overlap_minutes
            + 5.0 * primary_late_after_ready
            + secondary_wait_after_ready
        )
    )


def _ordered_heavy_pair_indices(
    left_index: int,
    right_index: int,
    *,
    row_complexities: dict[int, float],
    rows: list[SlottedDagPlanInput],
) -> tuple[int, int]:
    primary_index, secondary_index = sorted(
        (left_index, right_index),
        key=lambda index: (
            -row_complexities[index],
            rows[index].upstream_ready_minute,
            rows[index].dag_id,
        ),
    )
    return primary_index, secondary_index


def _configured_pair_indices(
    rows: list[SlottedDagPlanInput],
    configured_pairs: tuple[tuple[str, str], ...],
) -> list[tuple[int, int]]:
    row_index_by_dag_id = {row.dag_id: index for index, row in enumerate(rows)}
    pair_indices: list[tuple[int, int]] = []
    for primary_dag_id, secondary_dag_id in configured_pairs:
        primary_index = row_index_by_dag_id.get(primary_dag_id)
        secondary_index = row_index_by_dag_id.get(secondary_dag_id)
        if primary_index is None or secondary_index is None or primary_index == secondary_index:
            continue
        pair_indices.append((primary_index, secondary_index))
    return pair_indices


def _order_rows_with_precedence(
    rows: list[SlottedDagPlanInput],
    precedence_pairs: list[tuple[str, str]],
) -> list[SlottedDagPlanInput]:
    if not precedence_pairs:
        return rows

    row_by_dag_id = {row.dag_id: row for row in rows}
    incoming_count = {row.dag_id: 0 for row in rows}
    outgoing_by_dag_id = {row.dag_id: [] for row in rows}
    seen_edges: set[tuple[str, str]] = set()

    for primary_dag_id, secondary_dag_id in precedence_pairs:
        if primary_dag_id not in row_by_dag_id or secondary_dag_id not in row_by_dag_id:
            continue
        if primary_dag_id == secondary_dag_id or (primary_dag_id, secondary_dag_id) in seen_edges:
            continue
        seen_edges.add((primary_dag_id, secondary_dag_id))
        outgoing_by_dag_id[primary_dag_id].append(secondary_dag_id)
        incoming_count[secondary_dag_id] += 1

    original_rank = {row.dag_id: index for index, row in enumerate(rows)}
    ready_dag_ids = [dag_id for dag_id, count in incoming_count.items() if count == 0]
    ready_dag_ids.sort(key=lambda dag_id: original_rank[dag_id])
    ordered_rows: list[SlottedDagPlanInput] = []

    while ready_dag_ids:
        dag_id = ready_dag_ids.pop(0)
        ordered_rows.append(row_by_dag_id[dag_id])
        for secondary_dag_id in sorted(outgoing_by_dag_id[dag_id], key=lambda child: original_rank[child]):
            incoming_count[secondary_dag_id] -= 1
            if incoming_count[secondary_dag_id] == 0:
                ready_dag_ids.append(secondary_dag_id)
        ready_dag_ids.sort(key=lambda ready_dag_id: original_rank[ready_dag_id])

    return ordered_rows if len(ordered_rows) == len(rows) else rows


def _configured_sequential_pair_indices(
    rows: list[SlottedDagPlanInput],
    configured_pairs: tuple[tuple[str, str], ...],
) -> list[tuple[int, int]]:
    row_index_by_dag_id = {row.dag_id: index for index, row in enumerate(rows)}
    ordered_pairs: list[tuple[int, int]] = []
    for primary_dag_id, secondary_dag_id in configured_pairs:
        primary_index = row_index_by_dag_id.get(primary_dag_id)
        secondary_index = row_index_by_dag_id.get(secondary_dag_id)
        if primary_index is None or secondary_index is None or primary_index == secondary_index:
            continue
        ordered_pairs.append((primary_index, secondary_index))
    return ordered_pairs


def _background_peak_by_bucket(
    *,
    bucket_minutes: int,
    global_peak_by_minute: GlobalPressureProfile | None,
    assigned_load_windows: list[tuple[int, int, float, float] | tuple[int, int, float, float, GlobalPressureProfile]],
) -> dict[int, int]:
    background = {
        minute_of_day: int(round(global_peak_by_minute.get(minute_of_day, 0.0))) if global_peak_by_minute else 0
        for minute_of_day in range(0, 24 * 60, bucket_minutes)
    }
    for assigned_window in assigned_load_windows:
        if len(assigned_window) == 5:
            _, _, _, assigned_peak, assigned_peak_profile = assigned_window
            if assigned_peak_profile:
                for minute_of_day, peak_value in assigned_peak_profile.items():
                    background[minute_of_day] = background.get(minute_of_day, 0) + int(round(peak_value))
                continue
        assigned_start, assigned_finish, _, assigned_peak = assigned_window[:4]
        for bucket_minute in iter_window_buckets(assigned_start, assigned_finish, bucket_minutes):
            minute_of_day = bucket_minute % (24 * 60)
            background[minute_of_day] = background.get(minute_of_day, 0) + int(round(assigned_peak))
    return background


def _background_effective_starts_by_bucket(
    *,
    bucket_minutes: int,
    background_effective_starts_by_minute: GlobalPressureProfile | None,
    assigned_effective_starts: list[int],
) -> dict[int, int]:
    background = {
        minute_of_day: int(round(background_effective_starts_by_minute.get(minute_of_day, 0.0)))
        if background_effective_starts_by_minute
        else 0
        for minute_of_day in range(0, 24 * 60, bucket_minutes)
    }
    for effective_start in assigned_effective_starts:
        minute_of_day = round_down_to_bucket(effective_start, bucket_minutes) % (24 * 60)
        background[minute_of_day] = background.get(minute_of_day, 0) + 1
    return background


def _solve_with_greedy(
    rows: list[SlottedDagPlanInput],
    *,
    solver_config: SchedulingSolverConfig,
    working_hours: WorkingHours,
    bucket_minutes: int,
    min_gap_minutes: int,
    finish_deadline_minute: int,
    assigned_effective_starts: list[int],
    assigned_load_windows: list[tuple[int, int, float, float] | tuple[int, int, float, float, GlobalPressureProfile]],
    global_pressure_by_minute: GlobalPressureProfile | None,
    global_peak_by_minute: GlobalPressureProfile | None,
    observed_global_peak_target_by_minute: GlobalPressureProfile | None,
    background_effective_starts_by_minute: GlobalPressureProfile | None,
    observed_global_effective_start_target_by_minute: GlobalPressureProfile | None,
    observed_per_dag_task_peaks: dict[str, ObservedPeak],
    observed_per_dag_task_peak_profiles: dict[str, GlobalPressureProfile],
    parallelism_limit: int | None,
) -> list[SlottedDagAssignment]:
    local_effective_starts = list(assigned_effective_starts)
    local_load_windows = list(assigned_load_windows)
    assignments: list[SlottedDagAssignment] = []
    row_complexities = {
        row.dag_id: _row_complexity_score(
            row,
            observed_per_dag_task_peaks.get(row.dag_id),
        )
        for row in rows
    }
    ordered_sequential_pairs = list(solver_config.sequential_dag_pairs)
    ordered_dependency_gate_pairs = list(solver_config.dependency_gate_pairs)
    heavy_pair_dag_ids: tuple[str, str] | None = None
    if ordered_sequential_pairs:
        heavy_pair_dag_ids = ordered_sequential_pairs[0]
    elif solver_config.objective_mode == "concurrency_first" and len(rows) >= 2:
        ranked_rows = sorted(
            rows,
            key=lambda row: (
                -row_complexities[row.dag_id],
                row.upstream_ready_minute,
                row.dag_id,
            ),
        )
        heavy_pair_dag_ids = (ranked_rows[0].dag_id, ranked_rows[1].dag_id)
    assigned_finish_by_dag_id: dict[str, int] = {}

    if solver_config.objective_mode == "concurrency_first":
        ordered_rows = sorted(
            rows,
            key=lambda row: (
                -row_complexities[row.dag_id],
                row.upstream_ready_minute,
                row.dag_id,
            ),
        )
    else:
        ordered_rows = sorted(rows, key=slotted_row_sort_key)

    precedence_pairs = list(ordered_sequential_pairs) + list(ordered_dependency_gate_pairs)
    if heavy_pair_dag_ids is not None:
        precedence_pairs.append(heavy_pair_dag_ids)
    ordered_rows = _order_rows_with_precedence(ordered_rows, precedence_pairs)

    for row in ordered_rows:
        forced_upstream_ready_minute = row.upstream_ready_minute
        minimum_primary_start_minute: int | None = None
        if (
            heavy_pair_dag_ids is not None
            and row.dag_id == heavy_pair_dag_ids[1]
            and heavy_pair_dag_ids[0] in assigned_finish_by_dag_id
        ):
            forced_upstream_ready_minute = max(forced_upstream_ready_minute, assigned_finish_by_dag_id[heavy_pair_dag_ids[0]])
        dependency_gate_pair = next(
            (
                (primary_dag_id, secondary_dag_id)
                for primary_dag_id, secondary_dag_id in ordered_dependency_gate_pairs
                if row.dag_id == secondary_dag_id and primary_dag_id in assigned_finish_by_dag_id
            ),
            None,
        )
        if dependency_gate_pair is not None:
            primary_finish_minute = assigned_finish_by_dag_id[dependency_gate_pair[0]]
            minimum_primary_start_minute = max(
                0,
                primary_finish_minute - row.dependency_gate_offset_minutes,
            )
        primary_start, effective_start = choose_primary_start_slot(
            current_primary_start_minute=row.current_primary_start_minute,
            current_effective_start_minute=row.current_effective_start_minute,
            assigned_effective_starts=local_effective_starts,
            assigned_load_windows=local_load_windows,
            global_pressure_by_minute=global_pressure_by_minute,
            global_peak_by_minute=global_peak_by_minute,
            working_hours=working_hours,
            bucket_minutes=bucket_minutes,
            min_gap_minutes=min_gap_minutes,
            finish_deadline_minute=finish_deadline_minute,
            effective_processing_minutes=row.effective_processing_minutes,
            upstream_ready_minute=forced_upstream_ready_minute,
            dependency_gate_offset_minutes=row.dependency_gate_offset_minutes,
            post_ready_setup_minutes=row.post_ready_setup_minutes,
            task_load_weight=task_load_weight(row.median_task_count),
            task_peak_estimate=float(
                observed_per_dag_task_peaks.get(row.dag_id, ObservedPeak(subject=row.dag_id, observed_peak=0, peak_time="")).observed_peak
            ),
            task_peak_profile_by_minute=observed_per_dag_task_peak_profiles.get(row.dag_id),
            observed_global_peak_target_by_minute=observed_global_peak_target_by_minute,
            background_effective_starts_by_minute=background_effective_starts_by_minute,
            observed_global_effective_start_target_by_minute=observed_global_effective_start_target_by_minute,
            objective_mode=solver_config.objective_mode,
            parallelism_limit=parallelism_limit,
            force_earliest_ready_slot=row.force_earliest_ready_slot,
            minimum_primary_start_minute=minimum_primary_start_minute,
        )
        shifted_peak_profile = _shift_peak_profile(
            observed_per_dag_task_peak_profiles.get(row.dag_id),
            effective_start - row.current_effective_start_minute,
        )
        local_effective_starts.append(effective_start)
        assigned_finish_by_dag_id[row.dag_id] = effective_start + row.effective_processing_minutes
        local_load_windows.append(
            (
                effective_start,
                effective_start + row.effective_processing_minutes,
                task_load_weight(row.median_task_count),
                float(observed_per_dag_task_peaks.get(row.dag_id, ObservedPeak(subject=row.dag_id, observed_peak=0, peak_time="")).observed_peak),
                shifted_peak_profile,
            )
        )
        assignments.append(
            SlottedDagAssignment(
                dag_id=row.dag_id,
                proposed_primary_start_minute=primary_start,
                proposed_effective_start_minute=effective_start,
                strategy="upstream_ready_slot_search",
            )
        )

    assignments_by_dag = {assignment.dag_id: assignment for assignment in assignments}
    return [assignments_by_dag[row.dag_id] for row in rows if row.dag_id in assignments_by_dag]


def _evaluate_assignment_global_target_excess(
    assignments: list[SlottedDagAssignment],
    rows: list[SlottedDagPlanInput],
    *,
    bucket_minutes: int,
    global_peak_by_minute: GlobalPressureProfile | None,
    observed_global_peak_target_by_minute: GlobalPressureProfile | None,
    observed_per_dag_task_peaks: dict[str, ObservedPeak],
    observed_per_dag_task_peak_profiles: dict[str, GlobalPressureProfile],
    assigned_load_windows: list[tuple[int, int, float, float] | tuple[int, int, float, float, GlobalPressureProfile]],
) -> dict[int, float]:
    background_peak = _background_peak_by_bucket(
        bucket_minutes=bucket_minutes,
        global_peak_by_minute=global_peak_by_minute,
        assigned_load_windows=assigned_load_windows,
    )
    rows_by_dag_id = {row.dag_id: row for row in rows}
    assignment_peak_profiles: list[dict[int, int]] = []
    for assignment in assignments:
        row = rows_by_dag_id[assignment.dag_id]
        peak_profile = observed_per_dag_task_peak_profiles.get(row.dag_id)
        if peak_profile:
            assignment_peak_profiles.append(
                _shift_peak_profile(
                    peak_profile,
                    assignment.proposed_effective_start_minute - row.current_effective_start_minute,
                )
            )
            continue
        peak_estimate = int(
            round(float(observed_per_dag_task_peaks.get(row.dag_id, ObservedPeak(row.dag_id, 0, "")).observed_peak))
        )
        fallback_profile: dict[int, int] = {}
        for bucket_minute in iter_window_buckets(
            assignment.proposed_effective_start_minute,
            assignment.proposed_effective_start_minute + row.effective_processing_minutes,
            bucket_minutes,
        ):
            fallback_profile[bucket_minute % (24 * 60)] = peak_estimate
        assignment_peak_profiles.append(fallback_profile)

    excess_by_bucket: dict[int, float] = {}
    target_profile = observed_global_peak_target_by_minute or {}
    for bucket_minute in range(0, 24 * 60, bucket_minutes):
        bucket_load = background_peak.get(bucket_minute, 0)
        for profile in assignment_peak_profiles:
            bucket_load += profile.get(bucket_minute, 0)
        excess_by_bucket[bucket_minute] = max(0.0, bucket_load - target_profile.get(bucket_minute, 0.0))
    return excess_by_bucket


def _evaluate_assignment_effective_start_target_excess(
    assignments: list[SlottedDagAssignment],
    *,
    bucket_minutes: int,
    background_effective_starts_by_minute: GlobalPressureProfile | None,
    observed_global_effective_start_target_by_minute: GlobalPressureProfile | None,
    assigned_effective_starts: list[int],
) -> dict[int, float]:
    start_counts = _background_effective_starts_by_bucket(
        bucket_minutes=bucket_minutes,
        background_effective_starts_by_minute=background_effective_starts_by_minute,
        assigned_effective_starts=assigned_effective_starts,
    )
    for assignment in assignments:
        minute_of_day = round_down_to_bucket(assignment.proposed_effective_start_minute, bucket_minutes) % (24 * 60)
        start_counts[minute_of_day] = start_counts.get(minute_of_day, 0) + 1

    target_profile = observed_global_effective_start_target_by_minute or {}
    return {
        bucket_minute: max(0.0, start_counts.get(bucket_minute, 0) - target_profile.get(bucket_minute, 0.0))
        for bucket_minute in range(0, 24 * 60, bucket_minutes)
    }


def _evaluate_assignment_global_peak_cap_excess(
    assignments: list[SlottedDagAssignment],
    rows: list[SlottedDagPlanInput],
    *,
    bucket_minutes: int,
    global_peak_by_minute: GlobalPressureProfile | None,
    observed_global_peak_target_by_minute: GlobalPressureProfile | None,
    observed_per_dag_task_peaks: dict[str, ObservedPeak],
    observed_per_dag_task_peak_profiles: dict[str, GlobalPressureProfile],
    assigned_load_windows: list[tuple[int, int, float, float] | tuple[int, int, float, float, GlobalPressureProfile]],
) -> float:
    excess_by_bucket = _evaluate_assignment_global_target_excess(
        assignments,
        rows,
        bucket_minutes=bucket_minutes,
        global_peak_by_minute=global_peak_by_minute,
        observed_global_peak_target_by_minute={
            minute_of_day: max(observed_global_peak_target_by_minute.values())
            for minute_of_day in range(0, 24 * 60, bucket_minutes)
        }
        if observed_global_peak_target_by_minute
        else None,
        observed_per_dag_task_peaks=observed_per_dag_task_peaks,
        observed_per_dag_task_peak_profiles=observed_per_dag_task_peak_profiles,
        assigned_load_windows=assigned_load_windows,
    )
    return max(excess_by_bucket.values(), default=0.0)


def _evaluate_assignment_effective_start_cap_excess(
    assignments: list[SlottedDagAssignment],
    *,
    bucket_minutes: int,
    background_effective_starts_by_minute: GlobalPressureProfile | None,
    observed_global_effective_start_target_by_minute: GlobalPressureProfile | None,
    assigned_effective_starts: list[int],
) -> float:
    start_excess_by_bucket = _evaluate_assignment_effective_start_target_excess(
        assignments,
        bucket_minutes=bucket_minutes,
        background_effective_starts_by_minute=background_effective_starts_by_minute,
        observed_global_effective_start_target_by_minute={
            minute_of_day: max(observed_global_effective_start_target_by_minute.values())
            for minute_of_day in range(0, 24 * 60, bucket_minutes)
        }
        if observed_global_effective_start_target_by_minute
        else None,
        assigned_effective_starts=assigned_effective_starts,
    )
    return max(start_excess_by_bucket.values(), default=0.0)


def _cp_model_module():
    from ortools.sat.python import cp_model

    return cp_model


def _pywraplp_module():
    from ortools.linear_solver import pywraplp

    return pywraplp


def _solve_with_cp_sat_or_milp(
    rows: list[SlottedDagPlanInput],
    *,
    solver_config: SchedulingSolverConfig,
    working_hours: WorkingHours,
    bucket_minutes: int,
    min_gap_minutes: int,
    finish_deadline_minute: int,
    assigned_effective_starts: list[int],
    assigned_load_windows: list[tuple[int, int, float, float] | tuple[int, int, float, float, GlobalPressureProfile]],
    global_pressure_by_minute: GlobalPressureProfile | None,
    global_peak_by_minute: GlobalPressureProfile | None,
    observed_global_peak_target_by_minute: GlobalPressureProfile | None,
    background_effective_starts_by_minute: GlobalPressureProfile | None,
    observed_global_effective_start_target_by_minute: GlobalPressureProfile | None,
    observed_per_dag_task_peaks: dict[str, ObservedPeak],
    observed_per_dag_task_peak_profiles: dict[str, GlobalPressureProfile],
) -> list[SlottedDagAssignment]:
    candidates_by_row = _build_candidates(
        rows,
        objective_mode=solver_config.objective_mode,
        working_hours=working_hours,
        bucket_minutes=bucket_minutes,
        min_gap_minutes=min_gap_minutes,
        finish_deadline_minute=finish_deadline_minute,
        assigned_effective_starts=assigned_effective_starts,
        assigned_load_windows=assigned_load_windows,
        global_pressure_by_minute=global_pressure_by_minute,
        observed_per_dag_task_peaks=observed_per_dag_task_peaks,
        observed_per_dag_task_peak_profiles=observed_per_dag_task_peak_profiles,
    )
    background_peak = _background_peak_by_bucket(
        bucket_minutes=bucket_minutes,
        global_peak_by_minute=global_peak_by_minute,
        assigned_load_windows=assigned_load_windows,
    )
    background_effective_starts = _background_effective_starts_by_bucket(
        bucket_minutes=bucket_minutes,
        background_effective_starts_by_minute=background_effective_starts_by_minute,
        assigned_effective_starts=assigned_effective_starts,
    )
    observed_global_peak_cap = max(observed_global_peak_target_by_minute.values()) if observed_global_peak_target_by_minute else None
    observed_global_effective_start_cap = (
        max(observed_global_effective_start_target_by_minute.values())
        if observed_global_effective_start_target_by_minute
        else None
    )
    row_weights = {index: task_load_weight(row.median_task_count) for index, row in enumerate(rows)}
    row_complexities = {
        index: _row_complexity_score(row, observed_per_dag_task_peaks.get(row.dag_id))
        for index, row in enumerate(rows)
    }
    sequential_pair_indices = _configured_pair_indices(rows, solver_config.sequential_dag_pairs)
    if not sequential_pair_indices and solver_config.objective_mode == "concurrency_first" and len(rows) >= 2:
        ranked_indices = sorted(
            range(len(rows)),
            key=lambda index: (
                -row_complexities[index],
                rows[index].upstream_ready_minute,
                rows[index].dag_id,
            ),
        )
        sequential_pair_indices = [(ranked_indices[0], ranked_indices[1])]
    sequential_pair_lookup = {
        frozenset((primary_index, secondary_index)): (primary_index, secondary_index)
        for primary_index, secondary_index in sequential_pair_indices
    }
    dependency_gate_pair_lookup = {
        frozenset((primary_index, secondary_index)): (primary_index, secondary_index)
        for primary_index, secondary_index in _configured_pair_indices(rows, solver_config.dependency_gate_pairs)
    }
    bucket_domain = list(range(0, 24 * 60, bucket_minutes))
    soft_limit = None
    if solver_config.parallelism_limit is not None:
        soft_limit = int(round(solver_config.parallelism_limit * solver_config.soft_parallelism_fraction))

    if solver_config.backend == "cp_sat":
        cp_model = _cp_model_module()
        model = cp_model.CpModel()
        x_vars = {}
        for row_index, row_candidates in candidates_by_row.items():
            row_vars = []
            for candidate in row_candidates:
                var = model.NewBoolVar(f"x_{row_index}_{candidate.slot_index}")
                x_vars[(row_index, candidate.slot_index)] = var
                row_vars.append(var)
            model.Add(sum(row_vars) == 1)

        objective_terms = []
        for row_index, row_candidates in candidates_by_row.items():
            for candidate in row_candidates:
                objective_terms.append(candidate.base_cost * x_vars[(row_index, candidate.slot_index)])

        pair_vars = {}
        for left_index, right_index in combinations(range(len(rows)), 2):
            for left_candidate in candidates_by_row[left_index]:
                for right_candidate in candidates_by_row[right_index]:
                    pair_cost = _pair_cost(
                        left_candidate,
                        right_candidate,
                        row_weights[left_index],
                        row_weights[right_index],
                        min_gap_minutes,
                    )
                    ordered_pair = sequential_pair_lookup.get(frozenset((left_index, right_index)))
                    dependency_gate_pair = dependency_gate_pair_lookup.get(frozenset((left_index, right_index)))
                    if ordered_pair is not None:
                        primary_index, secondary_index = ordered_pair
                        primary_candidate = left_candidate if primary_index == left_index else right_candidate
                        secondary_candidate = right_candidate if secondary_index == right_index else left_candidate
                        if secondary_candidate.effective_start_minute < primary_candidate.finish_minute:
                            model.Add(
                                x_vars[(left_index, left_candidate.slot_index)]
                                + x_vars[(right_index, right_candidate.slot_index)]
                                <= 1
                            )
                            continue
                        pair_cost += _heavy_pair_sequence_cost(
                            rows[primary_index],
                            rows[secondary_index],
                            primary_candidate,
                            secondary_candidate,
                        )
                    if dependency_gate_pair is not None:
                        upstream_index, gated_index = dependency_gate_pair
                        upstream_candidate = left_candidate if upstream_index == left_index else right_candidate
                        gated_candidate = right_candidate if gated_index == right_index else left_candidate
                        gated_row = rows[gated_index]
                        if gated_candidate.effective_start_minute + gated_row.dependency_gate_offset_minutes < upstream_candidate.finish_minute:
                            model.Add(
                                x_vars[(left_index, left_candidate.slot_index)]
                                + x_vars[(right_index, right_candidate.slot_index)]
                                <= 1
                            )
                            continue
                    if pair_cost <= 0:
                        continue
                    pair_var = model.NewBoolVar(
                        f"pair_{left_index}_{left_candidate.slot_index}_{right_index}_{right_candidate.slot_index}"
                    )
                    pair_vars[(left_index, left_candidate.slot_index, right_index, right_candidate.slot_index)] = pair_var
                    model.Add(pair_var <= x_vars[(left_index, left_candidate.slot_index)])
                    model.Add(pair_var <= x_vars[(right_index, right_candidate.slot_index)])
                    model.Add(
                        pair_var
                        >= x_vars[(left_index, left_candidate.slot_index)]
                        + x_vars[(right_index, right_candidate.slot_index)]
                        - 1
                    )
                    objective_terms.append(pair_cost * pair_var)

        max_background_peak = max(background_peak.values(), default=0)
        max_task_peak = sum(
            max((candidate.peak_contribution for candidate in row_candidates), default=0)
            for row_candidates in candidates_by_row.values()
        )
        peak_bound = model.NewIntVar(0, max_background_peak + max_task_peak + 1, "peak_bound")
        for bucket_minute in bucket_domain:
            bucket_load = background_peak.get(bucket_minute, 0) + sum(
                dict(candidate.peak_profile_by_minute).get(bucket_minute, candidate.peak_contribution if bucket_minute in candidate.bucket_minutes else 0)
                * x_vars[(row_index, candidate.slot_index)]
                for row_index, row_candidates in candidates_by_row.items()
                for candidate in row_candidates
            )
            model.Add(peak_bound >= bucket_load)

        peak_bound_weight = (
            CONCURRENCY_FIRST_PEAK_BOUND_WEIGHT
            if solver_config.objective_mode == "concurrency_first"
            else PEAK_BOUND_WEIGHT
        )
        objective_terms.append(peak_bound_weight * peak_bound)

        if observed_global_peak_cap is not None:
            for bucket_minute in bucket_domain:
                bucket_load = background_peak.get(bucket_minute, 0) + sum(
                    dict(candidate.peak_profile_by_minute).get(
                        bucket_minute,
                        candidate.peak_contribution if bucket_minute in candidate.bucket_minutes else 0,
                    )
                    * x_vars[(row_index, candidate.slot_index)]
                    for row_index, row_candidates in candidates_by_row.items()
                    for candidate in row_candidates
                )
                target_load = int(round(observed_global_peak_cap))
                target_excess = model.NewIntVar(0, max_background_peak + max_task_peak + 1, f"target_excess_{bucket_minute}")
                model.Add(target_excess >= bucket_load - target_load)
                objective_terms.append(OBSERVED_PROFILE_EXCESS_WEIGHT * target_excess)

        if observed_global_effective_start_cap is not None:
            max_start_count = len(rows) + max(background_effective_starts.values(), default=0) + 1
            for bucket_minute in bucket_domain:
                start_count = background_effective_starts.get(bucket_minute, 0) + sum(
                    x_vars[(row_index, candidate.slot_index)]
                    for row_index, row_candidates in candidates_by_row.items()
                    for candidate in row_candidates
                    if candidate.effective_start_bucket_minute == bucket_minute
                )
                target_start_count = int(round(observed_global_effective_start_cap))
                start_excess = model.NewIntVar(0, max_start_count, f"start_excess_{bucket_minute}")
                model.Add(start_excess >= start_count - target_start_count)
                objective_terms.append(OBSERVED_PROFILE_EXCESS_WEIGHT * start_excess)

        if solver_config.parallelism_limit is not None:
            for bucket_minute in bucket_domain:
                bucket_load = background_peak.get(bucket_minute, 0) + sum(
                    candidate.peak_contribution * x_vars[(row_index, candidate.slot_index)]
                    for row_index, row_candidates in candidates_by_row.items()
                    for candidate in row_candidates
                    if bucket_minute in candidate.bucket_minutes
                )
                if soft_limit is not None:
                    soft_excess = model.NewIntVar(0, max_background_peak + max_task_peak + 1, f"soft_excess_{bucket_minute}")
                    model.Add(soft_excess >= bucket_load - soft_limit)
                    objective_terms.append(SOFT_EXCESS_WEIGHT * soft_excess)
                hard_excess = model.NewIntVar(0, max_background_peak + max_task_peak + 1, f"hard_excess_{bucket_minute}")
                model.Add(hard_excess >= bucket_load - solver_config.parallelism_limit)
                objective_terms.append(HARD_EXCESS_WEIGHT * hard_excess)

        model.Minimize(sum(objective_terms))
        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = solver_config.time_limit_seconds
        status = solver.Solve(model)
        if status not in {cp_model.OPTIMAL, cp_model.FEASIBLE}:
            raise RuntimeError(f"CP-SAT scheduling failed with status {solver.StatusName(status)}")

        assignments = []
        for row_index, row in enumerate(rows):
            chosen_candidate = next(
                candidate
                for candidate in candidates_by_row[row_index]
                if solver.Value(x_vars[(row_index, candidate.slot_index)]) == 1
            )
            assignments.append(
                SlottedDagAssignment(
                    dag_id=row.dag_id,
                    proposed_primary_start_minute=chosen_candidate.primary_start_minute,
                    proposed_effective_start_minute=chosen_candidate.effective_start_minute,
                    strategy="upstream_ready_cp_sat",
                )
            )
        return assignments

    pywraplp = _pywraplp_module()
    solver = None
    for backend_name in ("SCIP", "CBC_MIXED_INTEGER_PROGRAMMING", "SAT"):
        solver = pywraplp.Solver.CreateSolver(backend_name)
        if solver is not None:
            break
    if solver is None:
        raise RuntimeError("No OR-Tools MILP backend is available")
    solver.SetTimeLimit(int(solver_config.time_limit_seconds * 1000))

    x_vars = {}
    for row_index, row_candidates in candidates_by_row.items():
        row_vars = []
        for candidate in row_candidates:
            var = solver.BoolVar(f"x_{row_index}_{candidate.slot_index}")
            x_vars[(row_index, candidate.slot_index)] = var
            row_vars.append(var)
        solver.Add(solver.Sum(row_vars) == 1)

    objective = solver.Objective()
    for row_index, row_candidates in candidates_by_row.items():
        for candidate in row_candidates:
            objective.SetCoefficient(x_vars[(row_index, candidate.slot_index)], candidate.base_cost)

    for left_index, right_index in combinations(range(len(rows)), 2):
        for left_candidate in candidates_by_row[left_index]:
            for right_candidate in candidates_by_row[right_index]:
                pair_cost = _pair_cost(
                    left_candidate,
                    right_candidate,
                    row_weights[left_index],
                    row_weights[right_index],
                    min_gap_minutes,
                )
                ordered_pair = sequential_pair_lookup.get(frozenset((left_index, right_index)))
                dependency_gate_pair = dependency_gate_pair_lookup.get(frozenset((left_index, right_index)))
                if ordered_pair is not None:
                    primary_index, secondary_index = ordered_pair
                    primary_candidate = left_candidate if primary_index == left_index else right_candidate
                    secondary_candidate = right_candidate if secondary_index == right_index else left_candidate
                    if secondary_candidate.effective_start_minute < primary_candidate.finish_minute:
                        solver.Add(
                            x_vars[(left_index, left_candidate.slot_index)]
                            + x_vars[(right_index, right_candidate.slot_index)]
                            <= 1
                        )
                        continue
                    pair_cost += _heavy_pair_sequence_cost(
                        rows[primary_index],
                        rows[secondary_index],
                        primary_candidate,
                        secondary_candidate,
                    )
                if dependency_gate_pair is not None:
                    upstream_index, gated_index = dependency_gate_pair
                    upstream_candidate = left_candidate if upstream_index == left_index else right_candidate
                    gated_candidate = right_candidate if gated_index == right_index else left_candidate
                    gated_row = rows[gated_index]
                    if gated_candidate.effective_start_minute + gated_row.dependency_gate_offset_minutes < upstream_candidate.finish_minute:
                        solver.Add(
                            x_vars[(left_index, left_candidate.slot_index)]
                            + x_vars[(right_index, right_candidate.slot_index)]
                            <= 1
                        )
                        continue
                if pair_cost <= 0:
                    continue
                pair_var = solver.BoolVar(
                    f"pair_{left_index}_{left_candidate.slot_index}_{right_index}_{right_candidate.slot_index}"
                )
                solver.Add(pair_var <= x_vars[(left_index, left_candidate.slot_index)])
                solver.Add(pair_var <= x_vars[(right_index, right_candidate.slot_index)])
                solver.Add(
                    pair_var
                    >= x_vars[(left_index, left_candidate.slot_index)]
                    + x_vars[(right_index, right_candidate.slot_index)]
                    - 1
                )
                objective.SetCoefficient(pair_var, pair_cost)

    peak_bound = solver.NumVar(0.0, solver.infinity(), "peak_bound")
    peak_bound_weight = (
        CONCURRENCY_FIRST_PEAK_BOUND_WEIGHT
        if solver_config.objective_mode == "concurrency_first"
        else PEAK_BOUND_WEIGHT
    )
    objective.SetCoefficient(peak_bound, peak_bound_weight)
    for bucket_minute in bucket_domain:
        bucket_load = background_peak.get(bucket_minute, 0) + solver.Sum(
            dict(candidate.peak_profile_by_minute).get(bucket_minute, candidate.peak_contribution if bucket_minute in candidate.bucket_minutes else 0)
            * x_vars[(row_index, candidate.slot_index)]
            for row_index, row_candidates in candidates_by_row.items()
            for candidate in row_candidates
        )
        solver.Add(peak_bound >= bucket_load)
        if observed_global_peak_cap is not None:
            target_excess = solver.NumVar(0.0, solver.infinity(), f"target_excess_{bucket_minute}")
            solver.Add(target_excess >= bucket_load - observed_global_peak_cap)
            objective.SetCoefficient(target_excess, OBSERVED_PROFILE_EXCESS_WEIGHT)
        if observed_global_effective_start_cap is not None:
            start_count = background_effective_starts.get(bucket_minute, 0) + solver.Sum(
                x_vars[(row_index, candidate.slot_index)]
                for row_index, row_candidates in candidates_by_row.items()
                for candidate in row_candidates
                if candidate.effective_start_bucket_minute == bucket_minute
            )
            start_excess = solver.NumVar(0.0, solver.infinity(), f"start_excess_{bucket_minute}")
            solver.Add(start_excess >= start_count - observed_global_effective_start_cap)
            objective.SetCoefficient(start_excess, OBSERVED_PROFILE_EXCESS_WEIGHT)
        if solver_config.parallelism_limit is not None:
            if soft_limit is not None:
                soft_excess = solver.NumVar(0.0, solver.infinity(), f"soft_excess_{bucket_minute}")
                solver.Add(soft_excess >= bucket_load - soft_limit)
                objective.SetCoefficient(soft_excess, SOFT_EXCESS_WEIGHT)
            hard_excess = solver.NumVar(0.0, solver.infinity(), f"hard_excess_{bucket_minute}")
            solver.Add(hard_excess >= bucket_load - solver_config.parallelism_limit)
            objective.SetCoefficient(hard_excess, HARD_EXCESS_WEIGHT)

    objective.SetMinimization()
    status = solver.Solve()
    if status == pywraplp.Solver.NOT_SOLVED:
        return []
    if status not in {pywraplp.Solver.OPTIMAL, pywraplp.Solver.FEASIBLE}:
        raise RuntimeError(f"MILP scheduling failed with status {status}")

    assignments = []
    for row_index, row in enumerate(rows):
        chosen_candidate = next(
            candidate
            for candidate in candidates_by_row[row_index]
            if x_vars[(row_index, candidate.slot_index)].solution_value() > 0.5
        )
        assignments.append(
            SlottedDagAssignment(
                dag_id=row.dag_id,
                proposed_primary_start_minute=chosen_candidate.primary_start_minute,
                proposed_effective_start_minute=chosen_candidate.effective_start_minute,
                strategy="upstream_ready_milp",
            )
        )
    return assignments


def solve_slotted_rows(
    rows: list[SlottedDagPlanInput],
    *,
    solver_config: SchedulingSolverConfig,
    working_hours: WorkingHours,
    bucket_minutes: int,
    min_gap_minutes: int,
    finish_deadline_minute: int,
    assigned_effective_starts: list[int],
    assigned_load_windows: list[tuple[int, int, float, float] | tuple[int, int, float, float, GlobalPressureProfile]],
    global_pressure_by_minute: GlobalPressureProfile | None,
    global_peak_by_minute: GlobalPressureProfile | None,
    observed_global_peak_target_by_minute: GlobalPressureProfile | None = None,
    background_effective_starts_by_minute: GlobalPressureProfile | None = None,
    observed_global_effective_start_target_by_minute: GlobalPressureProfile | None = None,
    observed_per_dag_task_peaks: dict[str, ObservedPeak],
    observed_per_dag_task_peak_profiles: dict[str, GlobalPressureProfile],
) -> SchedulingSolveResult:
    if not rows:
        return SchedulingSolveResult(assignments=[], status="no_rows")
    if solver_config.backend == "greedy":
        assignments = _solve_with_greedy(
            rows,
            solver_config=solver_config,
            working_hours=working_hours,
            bucket_minutes=bucket_minutes,
            min_gap_minutes=min_gap_minutes,
            finish_deadline_minute=finish_deadline_minute,
            assigned_effective_starts=assigned_effective_starts,
            assigned_load_windows=assigned_load_windows,
            global_pressure_by_minute=global_pressure_by_minute,
            global_peak_by_minute=global_peak_by_minute,
            observed_global_peak_target_by_minute=observed_global_peak_target_by_minute,
            background_effective_starts_by_minute=background_effective_starts_by_minute,
            observed_global_effective_start_target_by_minute=observed_global_effective_start_target_by_minute,
            observed_per_dag_task_peaks=observed_per_dag_task_peaks,
            observed_per_dag_task_peak_profiles=observed_per_dag_task_peak_profiles,
            parallelism_limit=solver_config.parallelism_limit,
        )
    else:
        assignments = _solve_with_cp_sat_or_milp(
            rows,
            solver_config=solver_config,
            working_hours=working_hours,
            bucket_minutes=bucket_minutes,
            min_gap_minutes=min_gap_minutes,
            finish_deadline_minute=finish_deadline_minute,
            assigned_effective_starts=assigned_effective_starts,
            assigned_load_windows=assigned_load_windows,
            global_pressure_by_minute=global_pressure_by_minute,
            global_peak_by_minute=global_peak_by_minute,
            observed_global_peak_target_by_minute=observed_global_peak_target_by_minute,
            background_effective_starts_by_minute=background_effective_starts_by_minute,
            observed_global_effective_start_target_by_minute=observed_global_effective_start_target_by_minute,
            observed_per_dag_task_peaks=observed_per_dag_task_peaks,
            observed_per_dag_task_peak_profiles=observed_per_dag_task_peak_profiles,
        )

    if len(assignments) != len(rows):
        return SchedulingSolveResult(
            assignments=[],
            status="rejected",
            rejection_reason=f"{solver_config.backend}_not_solved",
        )

    if solver_config.objective_mode == "concurrency_first" and observed_global_peak_target_by_minute is not None:
        peak_cap_excess = _evaluate_assignment_global_peak_cap_excess(
            assignments,
            rows,
            bucket_minutes=bucket_minutes,
            global_peak_by_minute=global_peak_by_minute,
            observed_global_peak_target_by_minute=observed_global_peak_target_by_minute,
            observed_per_dag_task_peaks=observed_per_dag_task_peaks,
            observed_per_dag_task_peak_profiles=observed_per_dag_task_peak_profiles,
            assigned_load_windows=assigned_load_windows,
        )
        if peak_cap_excess > 0:
            return SchedulingSolveResult(
                assignments=[],
                status="rejected",
                rejection_reason="no_acceptable_concurrency_first_schedule",
            )

    if solver_config.objective_mode == "concurrency_first" and observed_global_effective_start_target_by_minute is not None:
        start_cap_excess = _evaluate_assignment_effective_start_cap_excess(
            assignments,
            bucket_minutes=bucket_minutes,
            background_effective_starts_by_minute=background_effective_starts_by_minute,
            observed_global_effective_start_target_by_minute=observed_global_effective_start_target_by_minute,
            assigned_effective_starts=assigned_effective_starts,
        )
        if start_cap_excess > 0:
            return SchedulingSolveResult(
                assignments=[],
                status="rejected",
                rejection_reason="no_acceptable_concurrency_first_schedule",
            )

    return SchedulingSolveResult(assignments=assignments, status="solved")