from __future__ import annotations

import csv
import json
from datetime import datetime

import pytest

from hypergraph_scheduler.optimizer import (
    ProposalRow,
    WorkingHours,
    build_scope_schedule_proposal,
    choose_primary_start_slot,
    format_duration_minutes,
    parse_cron_hours,
)
from hypergraph_scheduler.models import ObservedPeak, RepresentativeRunProfile, SchedulingSolverConfig, SlottedDagPlanInput
from hypergraph_scheduler.proposal import optimizer as proposal_optimizer
from hypergraph_scheduler.proposal.proposal_analysis import build_exact_shifted_peak_task_series, build_scoped_peak_task_series
from hypergraph_scheduler.schedule_solver import solve_slotted_rows
from hypergraph_scheduler.scopes import ScopeDefinition
from hypergraph_scheduler.scheduling.runtime_estimation import RuntimeEstimationConfig, proposal_effective_window_minutes


def test_choose_primary_start_slot_prefers_upstream_ready_window() -> None:
    slot, effective = choose_primary_start_slot(
        current_primary_start_minute=7 * 60 + 5,
        assigned_effective_starts=[],
        working_hours=WorkingHours(earliest_start_minute=8 * 60, latest_start_minute=18 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        effective_processing_minutes=443,
        upstream_ready_minute=10 * 60 + 30,
        post_ready_setup_minutes=9,
    )

    assert slot == 10 * 60 + 30
    assert effective == 10 * 60 + 39


def test_proposal_effective_window_minutes_respects_proposed_effective_start_for_create_config_anchor() -> None:
    proposal = ProposalRow(
        dag_id="plum",
        current_schedule="05 9 * * 1",
        proposed_schedule="45 13 * * 1",
        current_primary_start_utc="09:05",
        proposed_primary_start_utc="13:45",
        current_effective_start_utc="11:02",
        proposed_effective_start_utc="15:42",
        estimated_upstream_ready_utc="09:05",
        current_wait_before_ready_minutes=0,
        proposed_wait_before_ready_minutes=0,
        current_gap_after_ready_minutes=0,
        proposed_gap_after_ready_minutes=280,
        wait_saved_minutes=0,
        current_estimated_finish_utc="16:28",
        proposed_estimated_finish_utc="16:51",
        shift_minutes=280,
        pressure_buffer_minutes=1,
        effective_start_delay_minutes=504,
        post_ready_setup_minutes=117,
        direct_upstream_dependency_count=1,
        avg_dag_runtime_seconds=11058.7,
        median_dag_runtime_seconds=5862.0,
        p90_dag_runtime_seconds=27107.8,
        avg_effective_start_delay_seconds=30250.9,
        p90_effective_start_delay_seconds=72079.6,
        avg_effective_processing_seconds=4952.8,
        median_effective_processing_seconds=3846.4,
        p90_effective_processing_seconds=9267.9,
        total_scoped_idle_wait_seconds=26.4,
        mapped_upstream_idle_wait_seconds=26.4,
        mapped_edge_max_p90_idle_wait_seconds=6.5,
        mapped_edge_max_avg_ready_seconds=30249.1,
        mapped_edge_max_median_clipped_ready_seconds=14134.0,
        mapped_edge_max_p90_ready_seconds=72079.5,
        mapped_edge_max_avg_sensor_touch_seconds=30243.9,
        mapped_edge_max_p90_sensor_touch_seconds=72073.0,
        strategy="upstream_ready_cp_sat",
        recent_observed_effective_start_utc="09:35",
    )
    profile = RepresentativeRunProfile(
        dag_id="plum",
        run_id="scheduled__2026-05-12T09:05:00+00:00",
        logical_date="2026-05-12T09:05:00+00:00",
        anchor="create_config",
        start_delay_seconds=1800.0,
        processing_seconds=3846.4,
        schedule_to_end_seconds=5646.4,
    )
    runtime_estimation_config = RuntimeEstimationConfig(
        default_strategy="robust_runtime",
        task_sum_excluded_task_patterns=(),
        task_sum_excluded_operator_patterns=(),
        manual_overrides_seconds={},
        dependency_gate_offsets_seconds={},
    )

    current_effective_minutes, proposed_effective_minutes, processing_minutes = proposal_effective_window_minutes(
        proposal,
        profile,
        runtime_estimation_config,
    )

    assert current_effective_minutes == 11 * 60 + 2
    assert proposed_effective_minutes == 15 * 60 + 42
    assert proposed_effective_minutes + processing_minutes > proposed_effective_minutes


def test_choose_primary_start_slot_prefers_late_slot_over_gap_violation() -> None:
    slot, effective = choose_primary_start_slot(
        current_primary_start_minute=9 * 60,
        assigned_effective_starts=[9 * 60 + 5],
        working_hours=WorkingHours(earliest_start_minute=8 * 60, latest_start_minute=10 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        effective_processing_minutes=30,
        upstream_ready_minute=9 * 60,
        dependency_gate_offset_minutes=0,
        post_ready_setup_minutes=0,
    )

    assert slot == 10 * 60
    assert effective == slot


def test_choose_primary_start_slot_penalizes_heavy_overlap() -> None:
    slot, effective = choose_primary_start_slot(
        current_primary_start_minute=9 * 60,
        assigned_effective_starts=[11 * 60],
        assigned_load_windows=[(11 * 60, 13 * 60, 7.0)],
        working_hours=WorkingHours(earliest_start_minute=11 * 60, latest_start_minute=14 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        effective_processing_minutes=90,
        upstream_ready_minute=11 * 60,
        dependency_gate_offset_minutes=0,
        post_ready_setup_minutes=0,
        task_load_weight=10.0,
    )

    assert slot == 11 * 60 + 45
    assert effective == slot


def test_choose_primary_start_slot_penalizes_global_pressure() -> None:
    slot, effective = choose_primary_start_slot(
        current_primary_start_minute=9 * 60,
        assigned_effective_starts=[],
        global_pressure_by_minute={11 * 60: 20.0, 11 * 60 + 15: 20.0, 13 * 60: 2.0, 13 * 60 + 15: 2.0},
        working_hours=WorkingHours(earliest_start_minute=11 * 60, latest_start_minute=13 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        effective_processing_minutes=30,
        upstream_ready_minute=11 * 60,
        dependency_gate_offset_minutes=0,
        post_ready_setup_minutes=0,
        task_load_weight=10.0,
    )

    assert slot == 11 * 60 + 30
    assert effective == slot


def test_choose_primary_start_slot_penalizes_parallelism_cap_risk() -> None:
    slot, effective = choose_primary_start_slot(
        current_primary_start_minute=11 * 60,
        assigned_effective_starts=[],
        global_peak_by_minute={
            11 * 60: 20.0,
            11 * 60 + 15: 20.0,
            11 * 60 + 30: 20.0,
            11 * 60 + 45: 20.0,
            12 * 60: 2.0,
            12 * 60 + 15: 2.0,
        },
        working_hours=WorkingHours(earliest_start_minute=12 * 60, latest_start_minute=12 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        effective_processing_minutes=30,
        upstream_ready_minute=11 * 60,
        dependency_gate_offset_minutes=0,
        post_ready_setup_minutes=0,
        task_load_weight=4.0,
        task_peak_estimate=8.0,
        parallelism_limit=24,
    )

    assert slot == 12 * 60
    assert effective == slot


def test_choose_primary_start_slot_penalizes_global_peak_above_current_observed_profile() -> None:
    slot, effective = choose_primary_start_slot(
        current_primary_start_minute=12 * 60,
        assigned_effective_starts=[],
        global_peak_by_minute={11 * 60: 10.0, 11 * 60 + 15: 10.0, 12 * 60: 10.0, 12 * 60 + 15: 10.0},
        observed_global_peak_target_by_minute={11 * 60: 20.0, 11 * 60 + 15: 20.0, 12 * 60: 12.0, 12 * 60 + 15: 12.0},
        working_hours=WorkingHours(earliest_start_minute=11 * 60, latest_start_minute=12 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        effective_processing_minutes=30,
        upstream_ready_minute=11 * 60,
        dependency_gate_offset_minutes=0,
        post_ready_setup_minutes=0,
        task_load_weight=4.0,
        task_peak_estimate=8.0,
        task_peak_profile_by_minute={12 * 60: 8.0, 12 * 60 + 15: 8.0},
    )

    assert slot == 11 * 60
    assert effective == slot


def test_choose_primary_start_slot_penalizes_effective_start_burst_above_current_observed_profile() -> None:
    slot, effective = choose_primary_start_slot(
        current_primary_start_minute=12 * 60,
        assigned_effective_starts=[],
        background_effective_starts_by_minute={11 * 60: 0.0, 12 * 60: 1.0},
        observed_global_effective_start_target_by_minute={11 * 60: 1.0, 12 * 60: 1.0},
        working_hours=WorkingHours(earliest_start_minute=11 * 60, latest_start_minute=12 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        effective_processing_minutes=30,
        upstream_ready_minute=11 * 60,
        dependency_gate_offset_minutes=0,
        post_ready_setup_minutes=0,
        task_load_weight=4.0,
        objective_mode="concurrency_first",
    )

    assert slot == 11 * 60
    assert effective == slot


def test_estimate_upstream_ready_minute_caps_edge_estimate_with_recent_effective_start() -> None:
    upstream_ready_minute = proposal_optimizer._estimate_upstream_ready_minute(
        current_primary_start_minute=7 * 60 + 5,
        current_effective_start_minute=11 * 60 + 25,
        mapped_edge_max_median_clipped_ready_seconds=6 * 60 * 60 + 20 * 60,
        post_ready_setup_minutes=0,
        recent_observed_effective_start_minute=9 * 60 + 15,
    )

    assert upstream_ready_minute == 9 * 60 + 15


@pytest.mark.parametrize(
    ("backend", "expected_strategy"),
    [
        ("cp_sat", "upstream_ready_cp_sat"),
        ("milp", "upstream_ready_milp"),
    ],
)
def test_solve_slotted_rows_peak_aware_backends_avoid_parallelism_cap(
    backend: str,
    expected_strategy: str,
) -> None:
    row = SlottedDagPlanInput(
        dag_id="recipe_recommender",
        current_schedule="00 11 * * *",
        current_primary_start_minute=11 * 60,
        current_effective_start_minute=11 * 60,
        effective_start_delay_minutes=0,
        upstream_ready_minute=11 * 60,
        dependency_gate_offset_minutes=0,
        post_ready_setup_minutes=0,
        schedule_suffix="* * *",
        pressure_buffer_minutes=0,
        direct_upstream_dependency_count=0,
        avg_dag_runtime_seconds=0.0,
        median_dag_runtime_seconds=0.0,
        p90_dag_runtime_seconds=0.0,
        median_schedule_to_end_seconds=0.0,
        avg_effective_start_delay_seconds=0.0,
        p90_effective_start_delay_seconds=0.0,
        avg_effective_processing_seconds=0.0,
        median_effective_processing_seconds=0.0,
        p90_effective_processing_seconds=0.0,
        total_scoped_idle_wait_seconds=0.0,
        mapped_upstream_idle_wait_seconds=0.0,
        mapped_edge_max_p90_idle_wait_seconds=0.0,
        mapped_edge_max_avg_ready_seconds=0.0,
        mapped_edge_max_median_clipped_ready_seconds=0.0,
        mapped_edge_max_p90_ready_seconds=0.0,
        mapped_edge_max_avg_sensor_touch_seconds=0.0,
        mapped_edge_max_p90_sensor_touch_seconds=0.0,
        effective_processing_minutes=30,
        typical_processing_minutes=30,
        median_task_count=16.0,
    )

    assignments = solve_slotted_rows(
        [row],
        solver_config=SchedulingSolverConfig(
            backend=backend,
            objective_mode="wait_saving",
            parallelism_limit=24,
            soft_parallelism_fraction=0.75,
            time_limit_seconds=5.0,
        ),
        working_hours=WorkingHours(earliest_start_minute=11 * 60, latest_start_minute=12 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        assigned_effective_starts=[],
        assigned_load_windows=[],
        global_pressure_by_minute=None,
        global_peak_by_minute={
            11 * 60: 20.0,
            11 * 60 + 15: 20.0,
            11 * 60 + 30: 20.0,
            11 * 60 + 45: 20.0,
            12 * 60: 2.0,
        },
        observed_per_dag_task_peaks={
            "recipe_recommender": ObservedPeak(
                subject="recipe_recommender",
                observed_peak=8,
                peak_time="2026-05-07 11:00:00+00:00",
            )
        },
        observed_per_dag_task_peak_profiles={},
    )

    assert assignments.status == "solved"
    assert len(assignments.assignments) == 1
    assert assignments.assignments[0].proposed_primary_start_minute == 12 * 60
    assert assignments.assignments[0].proposed_effective_start_minute == 12 * 60
    assert assignments.assignments[0].strategy == expected_strategy


@pytest.mark.parametrize(
    ("backend", "expected_strategy"),
    [
        ("cp_sat", "upstream_ready_cp_sat"),
        ("milp", "upstream_ready_milp"),
    ],
)
def test_solve_slotted_rows_peak_aware_backends_penalize_global_peak_above_current_observed(
    backend: str,
    expected_strategy: str,
) -> None:
    row = SlottedDagPlanInput(
        dag_id="recipe_recommender",
        current_schedule="00 12 * * *",
        current_primary_start_minute=12 * 60,
        current_effective_start_minute=12 * 60,
        effective_start_delay_minutes=0,
        upstream_ready_minute=11 * 60,
        dependency_gate_offset_minutes=0,
        post_ready_setup_minutes=0,
        schedule_suffix="* * *",
        pressure_buffer_minutes=0,
        direct_upstream_dependency_count=0,
        avg_dag_runtime_seconds=0.0,
        median_dag_runtime_seconds=0.0,
        p90_dag_runtime_seconds=0.0,
        median_schedule_to_end_seconds=0.0,
        avg_effective_start_delay_seconds=0.0,
        p90_effective_start_delay_seconds=0.0,
        avg_effective_processing_seconds=0.0,
        median_effective_processing_seconds=0.0,
        p90_effective_processing_seconds=0.0,
        total_scoped_idle_wait_seconds=0.0,
        mapped_upstream_idle_wait_seconds=0.0,
        mapped_edge_max_p90_idle_wait_seconds=0.0,
        mapped_edge_max_avg_ready_seconds=0.0,
        mapped_edge_max_median_clipped_ready_seconds=0.0,
        mapped_edge_max_p90_ready_seconds=0.0,
        mapped_edge_max_avg_sensor_touch_seconds=0.0,
        mapped_edge_max_p90_sensor_touch_seconds=0.0,
        effective_processing_minutes=30,
        typical_processing_minutes=30,
        median_task_count=16.0,
    )

    assignments = solve_slotted_rows(
        [row],
        solver_config=SchedulingSolverConfig(
            backend=backend,
            objective_mode="wait_saving",
            parallelism_limit=24,
            soft_parallelism_fraction=0.75,
            time_limit_seconds=5.0,
        ),
        working_hours=WorkingHours(earliest_start_minute=11 * 60, latest_start_minute=12 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        assigned_effective_starts=[],
        assigned_load_windows=[],
        global_pressure_by_minute=None,
        global_peak_by_minute={11 * 60: 10.0, 11 * 60 + 15: 10.0, 12 * 60: 10.0, 12 * 60 + 15: 10.0},
        observed_global_peak_target_by_minute={11 * 60: 20.0, 11 * 60 + 15: 20.0, 12 * 60: 12.0, 12 * 60 + 15: 12.0},
        observed_per_dag_task_peaks={
            "recipe_recommender": ObservedPeak(
                subject="recipe_recommender",
                observed_peak=8,
                peak_time="2026-05-07 12:00:00+00:00",
            )
        },
        observed_per_dag_task_peak_profiles={"recipe_recommender": {12 * 60: 8.0, 12 * 60 + 15: 8.0}},
    )

    assert assignments.status == "solved"
    assert len(assignments.assignments) == 1
    assert assignments.assignments[0].proposed_primary_start_minute < 12 * 60
    assert assignments.assignments[0].proposed_effective_start_minute < 12 * 60
    assert assignments.assignments[0].strategy == expected_strategy


@pytest.mark.parametrize("backend", ["greedy", "cp_sat", "milp"])
def test_solve_slotted_rows_concurrency_first_rejects_worse_than_current_observed(backend: str) -> None:
    row = SlottedDagPlanInput(
        dag_id="recipe_recommender",
        current_schedule="00 12 * * *",
        current_primary_start_minute=12 * 60,
        current_effective_start_minute=12 * 60,
        effective_start_delay_minutes=0,
        upstream_ready_minute=11 * 60,
        dependency_gate_offset_minutes=0,
        post_ready_setup_minutes=0,
        schedule_suffix="* * *",
        pressure_buffer_minutes=0,
        direct_upstream_dependency_count=0,
        avg_dag_runtime_seconds=0.0,
        median_dag_runtime_seconds=0.0,
        p90_dag_runtime_seconds=0.0,
        median_schedule_to_end_seconds=0.0,
        avg_effective_start_delay_seconds=0.0,
        p90_effective_start_delay_seconds=0.0,
        avg_effective_processing_seconds=0.0,
        median_effective_processing_seconds=0.0,
        p90_effective_processing_seconds=0.0,
        total_scoped_idle_wait_seconds=0.0,
        mapped_upstream_idle_wait_seconds=0.0,
        mapped_edge_max_p90_idle_wait_seconds=0.0,
        mapped_edge_max_avg_ready_seconds=0.0,
        mapped_edge_max_median_clipped_ready_seconds=0.0,
        mapped_edge_max_p90_ready_seconds=0.0,
        mapped_edge_max_avg_sensor_touch_seconds=0.0,
        mapped_edge_max_p90_sensor_touch_seconds=0.0,
        effective_processing_minutes=30,
        typical_processing_minutes=30,
        median_task_count=16.0,
    )

    result = solve_slotted_rows(
        [row],
        solver_config=SchedulingSolverConfig(
            backend=backend,
            objective_mode="concurrency_first",
            parallelism_limit=24,
            soft_parallelism_fraction=0.75,
            time_limit_seconds=5.0,
        ),
        working_hours=WorkingHours(earliest_start_minute=12 * 60, latest_start_minute=12 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        assigned_effective_starts=[],
        assigned_load_windows=[],
        global_pressure_by_minute=None,
        global_peak_by_minute={11 * 60: 10.0, 11 * 60 + 15: 10.0, 12 * 60: 10.0, 12 * 60 + 15: 10.0},
        observed_global_peak_target_by_minute={11 * 60: 0.0, 11 * 60 + 15: 0.0, 12 * 60: 0.0, 12 * 60 + 15: 0.0},
        observed_per_dag_task_peaks={
            "recipe_recommender": ObservedPeak(
                subject="recipe_recommender",
                observed_peak=8,
                peak_time="2026-05-07 12:00:00+00:00",
            )
        },
        observed_per_dag_task_peak_profiles={"recipe_recommender": {12 * 60: 8.0, 12 * 60 + 15: 8.0}},
    )

    assert result.status == "rejected"
    assert result.rejection_reason == "no_acceptable_concurrency_first_schedule"
    assert result.assignments == []


@pytest.mark.parametrize("backend", ["greedy", "cp_sat", "milp"])
def test_solve_slotted_rows_concurrency_first_rejects_worse_than_current_effective_start_profile(backend: str) -> None:
    row = SlottedDagPlanInput(
        dag_id="recipe_recommender",
        current_schedule="00 12 * * *",
        current_primary_start_minute=12 * 60,
        current_effective_start_minute=12 * 60,
        effective_start_delay_minutes=0,
        upstream_ready_minute=11 * 60,
        dependency_gate_offset_minutes=0,
        post_ready_setup_minutes=0,
        schedule_suffix="* * *",
        pressure_buffer_minutes=0,
        direct_upstream_dependency_count=0,
        avg_dag_runtime_seconds=0.0,
        median_dag_runtime_seconds=0.0,
        p90_dag_runtime_seconds=0.0,
        median_schedule_to_end_seconds=0.0,
        avg_effective_start_delay_seconds=0.0,
        p90_effective_start_delay_seconds=0.0,
        avg_effective_processing_seconds=0.0,
        median_effective_processing_seconds=0.0,
        p90_effective_processing_seconds=0.0,
        total_scoped_idle_wait_seconds=0.0,
        mapped_upstream_idle_wait_seconds=0.0,
        mapped_edge_max_p90_idle_wait_seconds=0.0,
        mapped_edge_max_avg_ready_seconds=0.0,
        mapped_edge_max_median_clipped_ready_seconds=0.0,
        mapped_edge_max_p90_ready_seconds=0.0,
        mapped_edge_max_avg_sensor_touch_seconds=0.0,
        mapped_edge_max_p90_sensor_touch_seconds=0.0,
        effective_processing_minutes=30,
        typical_processing_minutes=30,
        median_task_count=16.0,
    )

    result = solve_slotted_rows(
        [row],
        solver_config=SchedulingSolverConfig(
            backend=backend,
            objective_mode="concurrency_first",
            parallelism_limit=24,
            soft_parallelism_fraction=0.75,
            time_limit_seconds=5.0,
        ),
        working_hours=WorkingHours(earliest_start_minute=12 * 60, latest_start_minute=12 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        assigned_effective_starts=[],
        assigned_load_windows=[],
        global_pressure_by_minute=None,
        global_peak_by_minute={11 * 60: 0.0, 12 * 60: 0.0},
        observed_global_peak_target_by_minute={11 * 60: 100.0, 12 * 60: 100.0},
        background_effective_starts_by_minute={11 * 60: 0.0, 12 * 60: 1.0},
        observed_global_effective_start_target_by_minute={11 * 60: 1.0, 12 * 60: 1.0},
        observed_per_dag_task_peaks={
            "recipe_recommender": ObservedPeak(
                subject="recipe_recommender",
                observed_peak=1,
                peak_time="2026-05-07 12:00:00+00:00",
            )
        },
        observed_per_dag_task_peak_profiles={"recipe_recommender": {12 * 60: 1.0}},
    )

    assert result.status == "rejected"
    assert result.rejection_reason == "no_acceptable_concurrency_first_schedule"
    assert result.assignments == []


@pytest.mark.parametrize("backend", ["greedy", "cp_sat", "milp"])
def test_solve_slotted_rows_concurrency_first_allows_redistribution_within_observed_caps(backend: str) -> None:
    row = SlottedDagPlanInput(
        dag_id="recipe_recommender",
        current_schedule="00 12 * * *",
        current_primary_start_minute=12 * 60,
        current_effective_start_minute=12 * 60,
        effective_start_delay_minutes=0,
        upstream_ready_minute=11 * 60,
        dependency_gate_offset_minutes=0,
        post_ready_setup_minutes=0,
        schedule_suffix="* * *",
        pressure_buffer_minutes=0,
        direct_upstream_dependency_count=0,
        avg_dag_runtime_seconds=0.0,
        median_dag_runtime_seconds=0.0,
        p90_dag_runtime_seconds=0.0,
        median_schedule_to_end_seconds=0.0,
        avg_effective_start_delay_seconds=0.0,
        p90_effective_start_delay_seconds=0.0,
        avg_effective_processing_seconds=0.0,
        median_effective_processing_seconds=0.0,
        p90_effective_processing_seconds=0.0,
        total_scoped_idle_wait_seconds=0.0,
        mapped_upstream_idle_wait_seconds=0.0,
        mapped_edge_max_p90_idle_wait_seconds=0.0,
        mapped_edge_max_avg_ready_seconds=0.0,
        mapped_edge_max_median_clipped_ready_seconds=0.0,
        mapped_edge_max_p90_ready_seconds=0.0,
        mapped_edge_max_avg_sensor_touch_seconds=0.0,
        mapped_edge_max_p90_sensor_touch_seconds=0.0,
        effective_processing_minutes=30,
        typical_processing_minutes=30,
        median_task_count=16.0,
    )

    result = solve_slotted_rows(
        [row],
        solver_config=SchedulingSolverConfig(
            backend=backend,
            objective_mode="concurrency_first",
            parallelism_limit=24,
            soft_parallelism_fraction=0.75,
            time_limit_seconds=5.0,
        ),
        working_hours=WorkingHours(earliest_start_minute=11 * 60, latest_start_minute=12 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        assigned_effective_starts=[],
        assigned_load_windows=[],
        global_pressure_by_minute=None,
        global_peak_by_minute={11 * 60: 10.0, 12 * 60: 10.0},
        observed_global_peak_target_by_minute={11 * 60: 12.0, 12 * 60: 20.0},
        background_effective_starts_by_minute={11 * 60: 1.0, 12 * 60: 0.0},
        observed_global_effective_start_target_by_minute={11 * 60: 1.0, 12 * 60: 3.0},
        observed_per_dag_task_peaks={
            "recipe_recommender": ObservedPeak(
                subject="recipe_recommender",
                observed_peak=8,
                peak_time="2026-05-07 12:00:00+00:00",
            )
        },
        observed_per_dag_task_peak_profiles={"recipe_recommender": {12 * 60: 8.0, 12 * 60 + 15: 8.0}},
    )

    assert result.status == "solved"
    assert len(result.assignments) == 1
    assert result.assignments[0].proposed_primary_start_minute < 12 * 60


@pytest.mark.parametrize("backend", ["greedy", "cp_sat", "milp"])
def test_solve_slotted_rows_ready_start_dag_uses_earliest_ready_slot(backend: str) -> None:
    row = SlottedDagPlanInput(
        dag_id="sales_forecast_v2",
        current_schedule="05 07 * * *",
        current_primary_start_minute=7 * 60 + 5,
        current_effective_start_minute=10 * 60 + 6,
        effective_start_delay_minutes=181,
        upstream_ready_minute=9 * 60 + 41,
        dependency_gate_offset_minutes=0,
        post_ready_setup_minutes=25,
        schedule_suffix="* * *",
        pressure_buffer_minutes=0,
        direct_upstream_dependency_count=1,
        avg_dag_runtime_seconds=0.0,
        median_dag_runtime_seconds=0.0,
        p90_dag_runtime_seconds=0.0,
        median_schedule_to_end_seconds=0.0,
        avg_effective_start_delay_seconds=0.0,
        p90_effective_start_delay_seconds=0.0,
        avg_effective_processing_seconds=0.0,
        median_effective_processing_seconds=0.0,
        p90_effective_processing_seconds=0.0,
        total_scoped_idle_wait_seconds=0.0,
        mapped_upstream_idle_wait_seconds=0.0,
        mapped_edge_max_p90_idle_wait_seconds=0.0,
        mapped_edge_max_avg_ready_seconds=0.0,
        mapped_edge_max_median_clipped_ready_seconds=0.0,
        mapped_edge_max_p90_ready_seconds=0.0,
        mapped_edge_max_avg_sensor_touch_seconds=0.0,
        mapped_edge_max_p90_sensor_touch_seconds=0.0,
        effective_processing_minutes=150,
        typical_processing_minutes=150,
        median_task_count=25.0,
        force_earliest_ready_slot=True,
    )

    result = solve_slotted_rows(
        [row],
        solver_config=SchedulingSolverConfig(
            backend=backend,
            objective_mode="concurrency_first",
            parallelism_limit=24,
            soft_parallelism_fraction=0.75,
            time_limit_seconds=5.0,
            ready_start_dag_ids=("sales_forecast_v2",),
        ),
        working_hours=WorkingHours(earliest_start_minute=8 * 60, latest_start_minute=18 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        assigned_effective_starts=[],
        assigned_load_windows=[],
        global_pressure_by_minute=None,
        global_peak_by_minute={minute: 8.0 for minute in range(8 * 60, 19 * 60, 15)},
        observed_global_peak_target_by_minute={minute: 24.0 for minute in range(8 * 60, 19 * 60, 15)},
        background_effective_starts_by_minute={minute: 0.0 for minute in range(8 * 60, 19 * 60, 15)},
        observed_global_effective_start_target_by_minute={minute: 3.0 for minute in range(8 * 60, 19 * 60, 15)},
        observed_per_dag_task_peaks={
            "sales_forecast_v2": ObservedPeak(
                subject="sales_forecast_v2",
                observed_peak=6,
                peak_time="2026-05-12 10:00:00+00:00",
            )
        },
        observed_per_dag_task_peak_profiles={"sales_forecast_v2": {10 * 60: 6.0, 10 * 60 + 15: 6.0}},
    )

    assert result.status == "solved"
    assert len(result.assignments) == 1
    assert result.assignments[0].proposed_primary_start_minute == 9 * 60 + 45


@pytest.mark.parametrize("backend", ["greedy", "cp_sat", "milp"])
def test_solve_slotted_rows_concurrency_first_sequences_heaviest_pair(backend: str) -> None:
    fork = SlottedDagPlanInput(
        dag_id="fork",
        current_schedule="05 07 * * *",
        current_primary_start_minute=7 * 60 + 5,
        current_effective_start_minute=9 * 60,
        effective_start_delay_minutes=115,
        upstream_ready_minute=9 * 60,
        dependency_gate_offset_minutes=0,
        post_ready_setup_minutes=0,
        schedule_suffix="* * *",
        pressure_buffer_minutes=0,
        direct_upstream_dependency_count=0,
        avg_dag_runtime_seconds=0.0,
        median_dag_runtime_seconds=0.0,
        p90_dag_runtime_seconds=0.0,
        median_schedule_to_end_seconds=0.0,
        avg_effective_start_delay_seconds=0.0,
        p90_effective_start_delay_seconds=0.0,
        avg_effective_processing_seconds=0.0,
        median_effective_processing_seconds=0.0,
        p90_effective_processing_seconds=0.0,
        total_scoped_idle_wait_seconds=0.0,
        mapped_upstream_idle_wait_seconds=0.0,
        mapped_edge_max_p90_idle_wait_seconds=0.0,
        mapped_edge_max_avg_ready_seconds=0.0,
        mapped_edge_max_median_clipped_ready_seconds=0.0,
        mapped_edge_max_p90_ready_seconds=0.0,
        mapped_edge_max_avg_sensor_touch_seconds=0.0,
        mapped_edge_max_p90_sensor_touch_seconds=0.0,
        effective_processing_minutes=240,
        typical_processing_minutes=240,
        median_task_count=100.0,
    )
    plum = SlottedDagPlanInput(
        dag_id="plum",
        current_schedule="05 09 * * 1",
        current_primary_start_minute=9 * 60 + 5,
        current_effective_start_minute=9 * 60 + 30,
        effective_start_delay_minutes=25,
        upstream_ready_minute=9 * 60,
        dependency_gate_offset_minutes=0,
        post_ready_setup_minutes=0,
        schedule_suffix="* * 1",
        pressure_buffer_minutes=0,
        direct_upstream_dependency_count=0,
        avg_dag_runtime_seconds=0.0,
        median_dag_runtime_seconds=0.0,
        p90_dag_runtime_seconds=0.0,
        median_schedule_to_end_seconds=0.0,
        avg_effective_start_delay_seconds=0.0,
        p90_effective_start_delay_seconds=0.0,
        avg_effective_processing_seconds=0.0,
        median_effective_processing_seconds=0.0,
        p90_effective_processing_seconds=0.0,
        total_scoped_idle_wait_seconds=0.0,
        mapped_upstream_idle_wait_seconds=0.0,
        mapped_edge_max_p90_idle_wait_seconds=0.0,
        mapped_edge_max_avg_ready_seconds=0.0,
        mapped_edge_max_median_clipped_ready_seconds=0.0,
        mapped_edge_max_p90_ready_seconds=0.0,
        mapped_edge_max_avg_sensor_touch_seconds=0.0,
        mapped_edge_max_p90_sensor_touch_seconds=0.0,
        effective_processing_minutes=90,
        typical_processing_minutes=90,
        median_task_count=25.0,
    )

    result = solve_slotted_rows(
        [fork, plum],
        solver_config=SchedulingSolverConfig(
            backend=backend,
            objective_mode="concurrency_first",
            parallelism_limit=24,
            soft_parallelism_fraction=0.75,
            time_limit_seconds=5.0,
        ),
        working_hours=WorkingHours(earliest_start_minute=9 * 60, latest_start_minute=14 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        assigned_effective_starts=[],
        assigned_load_windows=[],
        global_pressure_by_minute=None,
        global_peak_by_minute={minute: 8.0 for minute in range(9 * 60, 15 * 60, 15)},
        observed_global_peak_target_by_minute={minute: 24.0 for minute in range(9 * 60, 15 * 60, 15)},
        background_effective_starts_by_minute={minute: 0.0 for minute in range(9 * 60, 15 * 60, 15)},
        observed_global_effective_start_target_by_minute={minute: 3.0 for minute in range(9 * 60, 15 * 60, 15)},
        observed_per_dag_task_peaks={
            "fork": ObservedPeak(subject="fork", observed_peak=12, peak_time="2026-05-07 09:00:00+00:00"),
            "plum": ObservedPeak(subject="plum", observed_peak=6, peak_time="2026-05-07 09:00:00+00:00"),
        },
        observed_per_dag_task_peak_profiles={
            "fork": {minute: 12.0 for minute in range(9 * 60, 13 * 60, 15)},
            "plum": {minute: 6.0 for minute in range(9 * 60, 10 * 60 + 30, 15)},
        },
    )

    assert result.status == "solved"
    assignments = {assignment.dag_id: assignment for assignment in result.assignments}
    assert assignments["fork"].proposed_effective_start_minute < assignments["plum"].proposed_effective_start_minute
    assert assignments["fork"].proposed_effective_start_minute <= 10 * 60 + 30
    assert assignments["plum"].proposed_effective_start_minute >= assignments["fork"].proposed_effective_start_minute + 240


def test_choose_primary_start_slot_allows_earlier_start_before_dependency_gate() -> None:
    slot, effective = choose_primary_start_slot(
        current_primary_start_minute=7 * 60 + 5,
        assigned_effective_starts=[],
        working_hours=WorkingHours(earliest_start_minute=8 * 60, latest_start_minute=18 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        effective_processing_minutes=180,
        upstream_ready_minute=14 * 60 + 50,
        dependency_gate_offset_minutes=30,
        post_ready_setup_minutes=0,
    )

    assert slot == 14 * 60 + 30
    assert effective == slot


@pytest.mark.parametrize("backend", ["greedy", "cp_sat", "milp"])
def test_solve_slotted_rows_dependency_gate_pair_blocks_impossible_gate(backend: str) -> None:
    sales_forecast = SlottedDagPlanInput(
        dag_id="sales_forecast_v2",
        current_schedule="05 07 * * *",
        current_primary_start_minute=7 * 60 + 5,
        current_effective_start_minute=10 * 60 + 6,
        effective_start_delay_minutes=181,
        upstream_ready_minute=9 * 60 + 41,
        dependency_gate_offset_minutes=0,
        post_ready_setup_minutes=25,
        schedule_suffix="* * *",
        pressure_buffer_minutes=0,
        direct_upstream_dependency_count=1,
        avg_dag_runtime_seconds=0.0,
        median_dag_runtime_seconds=0.0,
        p90_dag_runtime_seconds=0.0,
        median_schedule_to_end_seconds=0.0,
        avg_effective_start_delay_seconds=0.0,
        p90_effective_start_delay_seconds=0.0,
        avg_effective_processing_seconds=0.0,
        median_effective_processing_seconds=0.0,
        p90_effective_processing_seconds=0.0,
        total_scoped_idle_wait_seconds=0.0,
        mapped_upstream_idle_wait_seconds=0.0,
        mapped_edge_max_p90_idle_wait_seconds=0.0,
        mapped_edge_max_avg_ready_seconds=0.0,
        mapped_edge_max_median_clipped_ready_seconds=0.0,
        mapped_edge_max_p90_ready_seconds=0.0,
        mapped_edge_max_avg_sensor_touch_seconds=0.0,
        mapped_edge_max_p90_sensor_touch_seconds=0.0,
        effective_processing_minutes=150,
        typical_processing_minutes=150,
        median_task_count=25.0,
        force_earliest_ready_slot=True,
    )
    fork = SlottedDagPlanInput(
        dag_id="fork",
        current_schedule="05 07 * * *",
        current_primary_start_minute=7 * 60 + 5,
        current_effective_start_minute=9 * 60,
        effective_start_delay_minutes=115,
        upstream_ready_minute=9 * 60 + 6,
        dependency_gate_offset_minutes=30,
        post_ready_setup_minutes=0,
        schedule_suffix="* * *",
        pressure_buffer_minutes=0,
        direct_upstream_dependency_count=2,
        avg_dag_runtime_seconds=0.0,
        median_dag_runtime_seconds=0.0,
        p90_dag_runtime_seconds=0.0,
        median_schedule_to_end_seconds=0.0,
        avg_effective_start_delay_seconds=0.0,
        p90_effective_start_delay_seconds=0.0,
        avg_effective_processing_seconds=0.0,
        median_effective_processing_seconds=0.0,
        p90_effective_processing_seconds=0.0,
        total_scoped_idle_wait_seconds=0.0,
        mapped_upstream_idle_wait_seconds=0.0,
        mapped_edge_max_p90_idle_wait_seconds=0.0,
        mapped_edge_max_avg_ready_seconds=0.0,
        mapped_edge_max_median_clipped_ready_seconds=0.0,
        mapped_edge_max_p90_ready_seconds=0.0,
        mapped_edge_max_avg_sensor_touch_seconds=0.0,
        mapped_edge_max_p90_sensor_touch_seconds=0.0,
        effective_processing_minutes=225,
        typical_processing_minutes=225,
        median_task_count=50.0,
    )

    result = solve_slotted_rows(
        [sales_forecast, fork],
        solver_config=SchedulingSolverConfig(
            backend=backend,
            objective_mode="wait_saving",
            parallelism_limit=24,
            soft_parallelism_fraction=0.75,
            time_limit_seconds=5.0,
            ready_start_dag_ids=("sales_forecast_v2",),
            dependency_gate_pairs=(("sales_forecast_v2", "fork"),),
        ),
        working_hours=WorkingHours(earliest_start_minute=8 * 60, latest_start_minute=18 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        assigned_effective_starts=[],
        assigned_load_windows=[],
        global_pressure_by_minute=None,
        global_peak_by_minute={minute: 8.0 for minute in range(8 * 60, 19 * 60, 15)},
        observed_global_peak_target_by_minute={minute: 24.0 for minute in range(8 * 60, 19 * 60, 15)},
        background_effective_starts_by_minute={minute: 0.0 for minute in range(8 * 60, 19 * 60, 15)},
        observed_global_effective_start_target_by_minute={minute: 3.0 for minute in range(8 * 60, 19 * 60, 15)},
        observed_per_dag_task_peaks={
            "sales_forecast_v2": ObservedPeak(subject="sales_forecast_v2", observed_peak=6, peak_time=""),
            "fork": ObservedPeak(subject="fork", observed_peak=10, peak_time=""),
        },
        observed_per_dag_task_peak_profiles={
            "sales_forecast_v2": {10 * 60: 6.0, 10 * 60 + 15: 6.0},
            "fork": {9 * 60: 10.0, 9 * 60 + 15: 10.0},
        },
    )

    assert result.status == "solved"
    assignment_by_dag = {assignment.dag_id: assignment for assignment in result.assignments}
    sales_assignment = assignment_by_dag["sales_forecast_v2"]
    fork_assignment = assignment_by_dag["fork"]
    sales_finish = sales_assignment.proposed_effective_start_minute + 150
    assert sales_assignment.proposed_primary_start_minute >= 9 * 60 + 45
    assert fork_assignment.proposed_effective_start_minute + 30 >= sales_finish


def test_proposal_row_to_dict_keeps_output_shape() -> None:
    result = ProposalRow(
        dag_id="recipe_recommender",
        current_schedule="05 07 * * 3",
        proposed_schedule="30 10 * * 3",
        current_primary_start_utc="07:05",
        proposed_primary_start_utc="10:30",
        current_effective_start_utc="10:39",
        proposed_effective_start_utc="10:39",
        estimated_upstream_ready_utc="10:30",
        current_wait_before_ready_minutes=205,
        proposed_wait_before_ready_minutes=0,
        current_gap_after_ready_minutes=0,
        proposed_gap_after_ready_minutes=0,
        wait_saved_minutes=205,
        current_estimated_finish_utc="18:02",
        proposed_estimated_finish_utc="18:02",
        shift_minutes=205,
        pressure_buffer_minutes=1,
        effective_start_delay_minutes=214,
        post_ready_setup_minutes=9,
        direct_upstream_dependency_count=3,
        avg_dag_runtime_seconds=30055.2,
        median_dag_runtime_seconds=26580.0,
        p90_dag_runtime_seconds=39863.4,
        avg_effective_start_delay_seconds=12840.0,
        p90_effective_start_delay_seconds=15000.0,
        avg_effective_processing_seconds=26580.0,
        median_effective_processing_seconds=26580.0,
        p90_effective_processing_seconds=30000.0,
        total_scoped_idle_wait_seconds=1000.0,
        mapped_upstream_idle_wait_seconds=1000.0,
        mapped_edge_max_p90_idle_wait_seconds=60.0,
        mapped_edge_max_avg_ready_seconds=30000.0,
        mapped_edge_max_median_clipped_ready_seconds=12300.0,
        mapped_edge_max_p90_ready_seconds=13200.0,
        mapped_edge_max_avg_sensor_touch_seconds=0.0,
        mapped_edge_max_p90_sensor_touch_seconds=0.0,
        strategy="upstream_ready_slot_search",
    )

    assert result.to_dict()["dag_id"] == "recipe_recommender"
    assert result.to_dict()["strategy"] == "upstream_ready_slot_search"


def test_build_scoped_peak_task_series_uses_historical_day_alignment() -> None:
    proposal_rows = [
        ProposalRow(
            dag_id="dag_a",
            current_schedule="00 10 * * *",
            proposed_schedule="00 11 * * *",
            current_primary_start_utc="10:00",
            proposed_primary_start_utc="11:00",
            current_effective_start_utc="10:00",
            proposed_effective_start_utc="11:00",
            estimated_upstream_ready_utc="10:00",
            current_wait_before_ready_minutes=0,
            proposed_wait_before_ready_minutes=0,
            current_gap_after_ready_minutes=0,
            proposed_gap_after_ready_minutes=0,
            wait_saved_minutes=0,
            current_estimated_finish_utc="10:15",
            proposed_estimated_finish_utc="11:15",
            shift_minutes=60,
            pressure_buffer_minutes=0,
            effective_start_delay_minutes=0,
            post_ready_setup_minutes=0,
            direct_upstream_dependency_count=0,
            avg_dag_runtime_seconds=0.0,
            median_dag_runtime_seconds=0.0,
            p90_dag_runtime_seconds=0.0,
            avg_effective_start_delay_seconds=0.0,
            p90_effective_start_delay_seconds=0.0,
            avg_effective_processing_seconds=0.0,
            median_effective_processing_seconds=0.0,
            p90_effective_processing_seconds=0.0,
            total_scoped_idle_wait_seconds=0.0,
            mapped_upstream_idle_wait_seconds=0.0,
            mapped_edge_max_p90_idle_wait_seconds=0.0,
            mapped_edge_max_avg_ready_seconds=0.0,
            mapped_edge_max_median_clipped_ready_seconds=0.0,
            mapped_edge_max_p90_ready_seconds=0.0,
            mapped_edge_max_avg_sensor_touch_seconds=0.0,
            mapped_edge_max_p90_sensor_touch_seconds=0.0,
            strategy="test",
        ),
        ProposalRow(
            dag_id="dag_b",
            current_schedule="00 10 * * *",
            proposed_schedule="00 11 * * *",
            current_primary_start_utc="10:00",
            proposed_primary_start_utc="11:00",
            current_effective_start_utc="10:00",
            proposed_effective_start_utc="11:00",
            estimated_upstream_ready_utc="10:00",
            current_wait_before_ready_minutes=0,
            proposed_wait_before_ready_minutes=0,
            current_gap_after_ready_minutes=0,
            proposed_gap_after_ready_minutes=0,
            wait_saved_minutes=0,
            current_estimated_finish_utc="10:15",
            proposed_estimated_finish_utc="11:15",
            shift_minutes=60,
            pressure_buffer_minutes=0,
            effective_start_delay_minutes=0,
            post_ready_setup_minutes=0,
            direct_upstream_dependency_count=0,
            avg_dag_runtime_seconds=0.0,
            median_dag_runtime_seconds=0.0,
            p90_dag_runtime_seconds=0.0,
            avg_effective_start_delay_seconds=0.0,
            p90_effective_start_delay_seconds=0.0,
            avg_effective_processing_seconds=0.0,
            median_effective_processing_seconds=0.0,
            p90_effective_processing_seconds=0.0,
            total_scoped_idle_wait_seconds=0.0,
            mapped_upstream_idle_wait_seconds=0.0,
            mapped_edge_max_p90_idle_wait_seconds=0.0,
            mapped_edge_max_avg_ready_seconds=0.0,
            mapped_edge_max_median_clipped_ready_seconds=0.0,
            mapped_edge_max_p90_ready_seconds=0.0,
            mapped_edge_max_avg_sensor_touch_seconds=0.0,
            mapped_edge_max_p90_sensor_touch_seconds=0.0,
            strategy="test",
        ),
    ]
    historical_profiles_by_day = {
        "2026-05-01": {
            "dag_a": RepresentativeRunProfile(
                dag_id="dag_a",
                run_id="run_a_1",
                logical_date="2026-05-01T10:00:00+00:00",
                anchor="dag_run",
                start_delay_seconds=0.0,
                processing_seconds=900.0,
                schedule_to_end_seconds=900.0,
            ),
            "dag_b": RepresentativeRunProfile(
                dag_id="dag_b",
                run_id="run_b_1",
                logical_date="2026-05-01T10:00:00+00:00",
                anchor="dag_run",
                start_delay_seconds=0.0,
                processing_seconds=900.0,
                schedule_to_end_seconds=900.0,
            ),
        },
        "2026-05-02": {
            "dag_a": RepresentativeRunProfile(
                dag_id="dag_a",
                run_id="run_a_2",
                logical_date="2026-05-02T10:00:00+00:00",
                anchor="dag_run",
                start_delay_seconds=0.0,
                processing_seconds=900.0,
                schedule_to_end_seconds=900.0,
            ),
            "dag_b": RepresentativeRunProfile(
                dag_id="dag_b",
                run_id="run_b_2",
                logical_date="2026-05-02T10:00:00+00:00",
                anchor="dag_run",
                start_delay_seconds=0.0,
                processing_seconds=900.0,
                schedule_to_end_seconds=900.0,
            ),
        },
    }
    task_intervals_by_run = {
        ("dag_a", "run_a_1"): [("2026-05-01T10:00:00+00:00", "2026-05-01T10:15:00+00:00")],
        ("dag_b", "run_b_1"): [("2026-05-01T10:00:00+00:00", "2026-05-01T10:15:00+00:00")],
        ("dag_a", "run_a_2"): [("2026-05-02T10:00:00+00:00", "2026-05-02T10:15:00+00:00")],
        ("dag_b", "run_b_2"): [("2026-05-02T10:00:00+00:00", "2026-05-02T10:15:00+00:00")],
    }

    current_series, proposed_series = build_scoped_peak_task_series(
        proposal_rows,
        historical_profiles_by_day,
        {
            key: [(datetime.fromisoformat(start), datetime.fromisoformat(end)) for start, end in intervals]
            for key, intervals in task_intervals_by_run.items()
        },
        bucket_minutes=15,
    )

    assert current_series[10 * 60] == 2.0
    assert proposed_series[11 * 60] == 2.0
    assert max(current_series.values()) == 2.0
    assert max(proposed_series.values()) == 2.0


def test_build_exact_shifted_peak_task_series_rigidly_shifts_real_intervals() -> None:
    proposal_rows = [
        ProposalRow(
            dag_id="dag_a",
            current_schedule="00 10 * * *",
            proposed_schedule="00 11 * * *",
            current_primary_start_utc="10:00",
            proposed_primary_start_utc="11:00",
            current_effective_start_utc="10:00",
            proposed_effective_start_utc="11:00",
            estimated_upstream_ready_utc="10:00",
            current_wait_before_ready_minutes=0,
            proposed_wait_before_ready_minutes=0,
            current_gap_after_ready_minutes=0,
            proposed_gap_after_ready_minutes=0,
            wait_saved_minutes=0,
            current_estimated_finish_utc="10:15",
            proposed_estimated_finish_utc="11:15",
            shift_minutes=60,
            pressure_buffer_minutes=0,
            effective_start_delay_minutes=0,
            post_ready_setup_minutes=0,
            direct_upstream_dependency_count=0,
            avg_dag_runtime_seconds=0.0,
            median_dag_runtime_seconds=0.0,
            p90_dag_runtime_seconds=0.0,
            avg_effective_start_delay_seconds=0.0,
            p90_effective_start_delay_seconds=0.0,
            avg_effective_processing_seconds=0.0,
            median_effective_processing_seconds=0.0,
            p90_effective_processing_seconds=0.0,
            total_scoped_idle_wait_seconds=0.0,
            mapped_upstream_idle_wait_seconds=0.0,
            mapped_edge_max_p90_idle_wait_seconds=0.0,
            mapped_edge_max_avg_ready_seconds=0.0,
            mapped_edge_max_median_clipped_ready_seconds=0.0,
            mapped_edge_max_p90_ready_seconds=0.0,
            mapped_edge_max_avg_sensor_touch_seconds=0.0,
            mapped_edge_max_p90_sensor_touch_seconds=0.0,
            strategy="test",
        )
    ]
    all_task_intervals_by_dag = {
        "dag_a": [
            (datetime.fromisoformat("2026-05-01T10:00:00+00:00"), datetime.fromisoformat("2026-05-01T10:15:00+00:00")),
        ],
        "other": [
            (datetime.fromisoformat("2026-05-01T10:00:00+00:00"), datetime.fromisoformat("2026-05-01T10:15:00+00:00")),
        ],
    }

    current_scoped, proposed_scoped, current_global, proposed_global = build_exact_shifted_peak_task_series(
        proposal_rows,
        {"dag_a": all_task_intervals_by_dag["dag_a"]},
        all_task_intervals_by_dag,
        bucket_minutes=15,
    )

    assert current_scoped[10 * 60] == 1.0
    assert proposed_scoped[11 * 60] == 1.0
    assert current_global[10 * 60] == 2.0
    assert proposed_global[10 * 60] == 1.0
    assert proposed_global[11 * 60] == 1.0


def test_parse_cron_hours_and_format_duration_minutes() -> None:
    minute, hours, suffix = parse_cron_hours("30 4,18 * * *")

    assert minute == 30
    assert hours == [4, 18]
    assert suffix == "* * *"
    assert format_duration_minutes(205) == "3h 25m"
    assert format_duration_minutes(60) == "1h"
    assert format_duration_minutes(5) == "5m"


class _FakeExecuteResult:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._rows


class _FakeConnection:
    def __init__(
        self,
        rows: list[tuple[object, ...]],
        diagnostic_rows: list[tuple[object, ...]] | None = None,
        representative_rows: list[tuple[object, ...]] | None = None,
        task_sum_rows: list[tuple[object, ...]] | None = None,
        task_count_rows: list[tuple[object, ...]] | None = None,
        global_pressure_rows: list[tuple[object, ...]] | None = None,
        observed_global_task_peak_rows: list[tuple[object, ...]] | None = None,
        observed_scoped_task_peak_rows: list[tuple[object, ...]] | None = None,
        observed_per_dag_task_peak_rows: list[tuple[object, ...]] | None = None,
        observed_per_dag_run_peak_rows: list[tuple[object, ...]] | None = None,
        observed_global_peak_profile_rows: list[tuple[object, ...]] | None = None,
        observed_global_effective_start_profile_rows: list[tuple[object, ...]] | None = None,
        observed_non_scoped_effective_start_profile_rows: list[tuple[object, ...]] | None = None,
        recent_observed_effective_start_minute_rows: list[tuple[object, ...]] | None = None,
        observed_non_scoped_peak_profile_rows: list[tuple[object, ...]] | None = None,
        observed_scoped_peak_profile_rows: list[tuple[object, ...]] | None = None,
        observed_per_dag_task_peak_profile_rows: list[tuple[object, ...]] | None = None,
        representative_task_interval_rows: list[tuple[object, ...]] | None = None,
        task_interval_rows: list[tuple[object, ...]] | None = None,
    ) -> None:
        self.rows = rows
        self.diagnostic_rows = diagnostic_rows or []
        self.representative_rows = representative_rows or []
        self.task_sum_rows = task_sum_rows or []
        self.task_count_rows = task_count_rows or []
        self.global_pressure_rows = global_pressure_rows or []
        self.observed_global_task_peak_rows = observed_global_task_peak_rows or []
        self.observed_scoped_task_peak_rows = observed_scoped_task_peak_rows or []
        self.observed_per_dag_task_peak_rows = observed_per_dag_task_peak_rows or []
        self.observed_per_dag_run_peak_rows = observed_per_dag_run_peak_rows or []
        self.observed_global_peak_profile_rows = observed_global_peak_profile_rows or []
        self.observed_global_effective_start_profile_rows = observed_global_effective_start_profile_rows or []
        self.observed_non_scoped_effective_start_profile_rows = observed_non_scoped_effective_start_profile_rows or []
        self.recent_observed_effective_start_minute_rows = recent_observed_effective_start_minute_rows or []
        self.observed_non_scoped_peak_profile_rows = observed_non_scoped_peak_profile_rows or []
        self.observed_scoped_peak_profile_rows = observed_scoped_peak_profile_rows or []
        self.observed_per_dag_task_peak_profile_rows = observed_per_dag_task_peak_profile_rows or []
        self.representative_task_interval_rows = representative_task_interval_rows or []
        self.task_interval_rows = task_interval_rows or []
        self.queries: list[str] = []

    def execute(self, query: str) -> _FakeExecuteResult:
        self.queries.append(query)
        if "RECENT_OBSERVED_EFFECTIVE_START_MINUTES" in query:
            return _FakeExecuteResult(self.recent_observed_effective_start_minute_rows)
        if "OBSERVED_GLOBAL_EFFECTIVE_START_PROFILE" in query:
            return _FakeExecuteResult(self.observed_global_effective_start_profile_rows)
        if "OBSERVED_NON_SCOPED_EFFECTIVE_START_PROFILE" in query:
            return _FakeExecuteResult(self.observed_non_scoped_effective_start_profile_rows)
        if "OBSERVED_GLOBAL_TASK_PEAK_PROFILE" in query:
            return _FakeExecuteResult(self.observed_global_peak_profile_rows)
        if "OBSERVED_NON_SCOPED_TASK_PEAK_PROFILE" in query:
            return _FakeExecuteResult(self.observed_non_scoped_peak_profile_rows)
        if "OBSERVED_SCOPED_TASK_PEAK_PROFILE" in query:
            return _FakeExecuteResult(self.observed_scoped_peak_profile_rows)
        if "OBSERVED_PER_DAG_TASK_PEAK_PROFILE" in query:
            return _FakeExecuteResult(self.observed_per_dag_task_peak_profile_rows)
        if "REPRESENTATIVE_TASK_INTERVALS" in query:
            return _FakeExecuteResult(self.representative_task_interval_rows)
        if "TASK_INTERVALS_BY_DAG" in query:
            return _FakeExecuteResult(self.task_interval_rows)
        if "OBSERVED_GLOBAL_TASK_PEAK" in query:
            return _FakeExecuteResult(self.observed_global_task_peak_rows)
        if "OBSERVED_SCOPED_TASK_PEAK" in query:
            return _FakeExecuteResult(self.observed_scoped_task_peak_rows)
        if "OBSERVED_PER_DAG_TASK_PEAK" in query:
            return _FakeExecuteResult(self.observed_per_dag_task_peak_rows)
        if "OBSERVED_PER_DAG_RUN_PEAK" in query:
            return _FakeExecuteResult(self.observed_per_dag_run_peak_rows)
        if "seed_edge_wait_runs" in query:
            return _FakeExecuteResult(self.diagnostic_rows)
        if "WITH create_config AS" in query:
            return _FakeExecuteResult(self.representative_rows)
        if "MEDIAN(task_count) AS median_task_count" in query:
            return _FakeExecuteResult(self.task_count_rows)
        if "WITH task_buckets AS" in query:
            return _FakeExecuteResult(self.global_pressure_rows)
        if "WITH task_sum_runs AS" in query:
            return _FakeExecuteResult(self.task_sum_rows)
        return _FakeExecuteResult(self.rows)


def test_build_schedule_proposal_writes_markdown_and_csv(monkeypatch, tmp_path) -> None:
    model_path = tmp_path / "recommendation_engine_schedule_optimization_model.json"
    model_path.write_text(
        json.dumps(
            {
                "optimization_defaults": {
                    "working_hours_constraint": {
                        "earliest_start": "08:00",
                        "latest_start": "18:00",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    rows = [
        (
            "recipe_recommender",
            "05 07 * * 3",
            3,
            30055.2,
            26580.0,
            39863.4,
            39180.0,
            214 * 60,
            250 * 60,
            443 * 60,
            443 * 60,
            500 * 60,
            1000.0,
            1000.0,
            60.0,
            500 * 60,
            205 * 60,
            220 * 60,
            0.0,
            0.0,
        ),
        (
            "menu_ranker",
            "30 4,18 * * *",
            0,
            805.4,
            805.5,
            1217.0,
            943.0,
            2 * 60,
            3 * 60,
            13 * 60,
            13 * 60,
            14 * 60,
            0.0,
            0.0,
            0.0,
            2 * 60,
            2 * 60,
            2 * 60,
            0.0,
            0.0,
        ),
    ]
    diagnostic_rows = [
        (
            "recipe_recommender",
            "recipe_feature_groups_sf",
            "wait_for_recipe_fg_sf",
            "scheduled__2026-05-07T07:05:00+00:00",
            "2026-05-07 07:05:00+00:00",
            25 * 60 * 60,
            20 * 60 * 60,
            True,
        )
    ]
    representative_rows = [
        (
            "recipe_recommender",
            "scheduled__2026-05-07T07:05:00+00:00",
            "2026-05-07 07:05:00+00:00",
            214 * 60,
            500 * 60,
            653 * 60,
            214 * 60,
            443 * 60,
        ),
        (
            "menu_ranker",
            "scheduled__2026-05-07T04:30:00+00:00",
            "2026-05-07 04:30:00+00:00",
            2 * 60,
            13 * 60,
            15 * 60,
            None,
            None,
        ),
    ]
    task_sum_rows = [
        ("menu_ranker", 1, 12 * 60, 12 * 60, 12 * 60),
        ("recipe_recommender", 1, 205 * 60, 205 * 60, 205 * 60),
    ]
    task_count_rows = [
        ("menu_ranker", 24.0),
        ("recipe_recommender", 80.0),
    ]
    global_pressure_rows = [
        (270, 2.0),
        (630, 20.0),
    ]
    observed_global_task_peak_rows = [(22, "2026-05-07 10:32:00+00:00")]
    observed_scoped_task_peak_rows = [(6, "2026-05-07 10:35:00+00:00")]
    observed_per_dag_task_peak_rows = [
        ("menu_ranker", 2, "2026-05-07 04:33:00+00:00"),
        ("recipe_recommender", 7, "2026-05-07 10:40:00+00:00"),
    ]
    observed_per_dag_run_peak_rows = [
        ("menu_ranker", 1, "2026-05-07 04:30:00+00:00"),
        ("recipe_recommender", 1, "2026-05-07 07:05:00+00:00"),
    ]
    observed_global_peak_profile_rows = [(270, 24.0), (630, 22.0)]
    recent_observed_effective_start_minute_rows = [
        ("menu_ranker", 272),
        ("recipe_recommender", 639),
    ]
    observed_non_scoped_peak_profile_rows = [(270, 20.0), (630, 16.0)]
    observed_scoped_peak_profile_rows = [(270, 2.0), (630, 6.0)]
    observed_per_dag_task_peak_profile_rows = [
        ("menu_ranker", 270, 2.0),
        ("recipe_recommender", 630, 7.0),
    ]
    representative_task_interval_rows = [
        ("menu_ranker", "scheduled__2026-05-07T04:30:00+00:00", "2026-05-07 04:30:00+00:00", "2026-05-07 04:45:00+00:00"),
        ("menu_ranker", "scheduled__2026-05-07T04:30:00+00:00", "2026-05-07 04:30:00+00:00", "2026-05-07 04:45:00+00:00"),
        ("recipe_recommender", "scheduled__2026-05-07T07:05:00+00:00", "2026-05-07 10:39:00+00:00", "2026-05-07 10:54:00+00:00"),
        ("recipe_recommender", "scheduled__2026-05-07T07:05:00+00:00", "2026-05-07 10:39:00+00:00", "2026-05-07 10:54:00+00:00"),
        ("recipe_recommender", "scheduled__2026-05-07T07:05:00+00:00", "2026-05-07 10:39:00+00:00", "2026-05-07 10:54:00+00:00"),
        ("recipe_recommender", "scheduled__2026-05-07T07:05:00+00:00", "2026-05-07 10:39:00+00:00", "2026-05-07 10:54:00+00:00"),
        ("recipe_recommender", "scheduled__2026-05-07T07:05:00+00:00", "2026-05-07 10:39:00+00:00", "2026-05-07 10:54:00+00:00"),
        ("recipe_recommender", "scheduled__2026-05-07T07:05:00+00:00", "2026-05-07 10:39:00+00:00", "2026-05-07 10:54:00+00:00"),
        ("recipe_recommender", "scheduled__2026-05-07T07:05:00+00:00", "2026-05-07 10:39:00+00:00", "2026-05-07 10:54:00+00:00"),
    ]
    task_interval_rows = [
        ("menu_ranker", "2026-05-07 04:30:00+00:00", "2026-05-07 04:45:00+00:00"),
        ("menu_ranker", "2026-05-07 04:30:00+00:00", "2026-05-07 04:45:00+00:00"),
        ("other_dag", "2026-05-07 10:30:00+00:00", "2026-05-07 10:45:00+00:00"),
        ("recipe_recommender", "2026-05-07 10:30:00+00:00", "2026-05-07 10:45:00+00:00"),
        ("recipe_recommender", "2026-05-07 10:30:00+00:00", "2026-05-07 10:45:00+00:00"),
        ("recipe_recommender", "2026-05-07 10:30:00+00:00", "2026-05-07 10:45:00+00:00"),
        ("recipe_recommender", "2026-05-07 10:30:00+00:00", "2026-05-07 10:45:00+00:00"),
        ("recipe_recommender", "2026-05-07 10:30:00+00:00", "2026-05-07 10:45:00+00:00"),
        ("recipe_recommender", "2026-05-07 10:30:00+00:00", "2026-05-07 10:45:00+00:00"),
        ("recipe_recommender", "2026-05-07 10:30:00+00:00", "2026-05-07 10:45:00+00:00"),
    ]
    connection = _FakeConnection(
        rows,
        diagnostic_rows=diagnostic_rows,
        representative_rows=representative_rows,
        task_sum_rows=task_sum_rows,
        task_count_rows=task_count_rows,
        global_pressure_rows=global_pressure_rows,
        observed_global_task_peak_rows=observed_global_task_peak_rows,
        observed_scoped_task_peak_rows=observed_scoped_task_peak_rows,
        observed_per_dag_task_peak_rows=observed_per_dag_task_peak_rows,
        observed_per_dag_run_peak_rows=observed_per_dag_run_peak_rows,
        observed_global_peak_profile_rows=observed_global_peak_profile_rows,
        recent_observed_effective_start_minute_rows=recent_observed_effective_start_minute_rows,
        observed_non_scoped_peak_profile_rows=observed_non_scoped_peak_profile_rows,
        observed_scoped_peak_profile_rows=observed_scoped_peak_profile_rows,
        observed_per_dag_task_peak_profile_rows=observed_per_dag_task_peak_profile_rows,
        representative_task_interval_rows=representative_task_interval_rows,
        task_interval_rows=task_interval_rows,
    )
    scope = ScopeDefinition(
        scope_id="monday_ds",
        display_name="Monday DS",
        input_dir=tmp_path,
        graph_path=tmp_path / "graph.json",
        model_path=model_path,
        artifact_prefix="monday_ds",
        seed_edge_sensor_map=[],
    )

    monkeypatch.setattr("hypergraph_scheduler.optimizer.ARTIFACTS_DIR", tmp_path)

    markdown_path = build_scope_schedule_proposal(connection, scope)

    csv_path = tmp_path / "monday_ds_schedule_proposal.csv"
    reviewed_assumptions_csv_path = tmp_path / "monday_ds_reviewed_assumptions.csv"
    reviewed_assumptions_markdown_path = tmp_path / "monday_ds_reviewed_assumptions.md"
    why_each_dag_moved_markdown_path = tmp_path / "monday_ds_why_each_dag_moved.md"
    hourly_pressure_csv_path = tmp_path / "monday_ds_hourly_pressure_parallel.csv"
    observed_global_limits_csv_path = tmp_path / "monday_ds_observed_global_limits.csv"
    observed_per_dag_limits_csv_path = tmp_path / "monday_ds_observed_per_dag_limits.csv"
    mermaid_chart_path = tmp_path / "monday_ds_pressure_parallel_evolution.mmd"
    global_mermaid_chart_path = tmp_path / "monday_ds_global_pressure_evolution.mmd"
    assert markdown_path == tmp_path / "monday_ds_schedule_proposal.md"
    assert markdown_path.exists()
    assert csv_path.exists()
    assert reviewed_assumptions_csv_path.exists()
    assert reviewed_assumptions_markdown_path.exists()
    assert why_each_dag_moved_markdown_path.exists()
    assert hourly_pressure_csv_path.exists()
    assert observed_global_limits_csv_path.exists()
    assert observed_per_dag_limits_csv_path.exists()
    assert mermaid_chart_path.exists()
    assert global_mermaid_chart_path.exists()
    assert "FROM monday_ds_optimization_inputs" in connection.queries[0]
    assert any("MEDIAN(task_count) AS median_task_count" in query for query in connection.queries)
    assert any("MEDIAN(running_task_count) AS median_running_task_count" in query for query in connection.queries)
    assert any("MAX(running_task_count) AS peak_running_task_count" in query for query in connection.queries)
    assert any("OBSERVED_NON_SCOPED_TASK_PEAK_PROFILE" in query for query in connection.queries)
    assert any("OBSERVED_PER_DAG_TASK_PEAK_PROFILE" in query for query in connection.queries)
    assert any("RECENT_OBSERVED_EFFECTIVE_START_MINUTES" in query for query in connection.queries)
    assert not any("FROM monday_ds_seed_edge_wait_runs" in query for query in connection.queries)
    assert not any(
        "FROM dag_runs_enriched dr" in query
        and "LEFT JOIN create_config cc" in query
        and "task_id IN ('create_config', 'create_run_config')" in query
        for query in connection.queries
    )
    assert len(connection.queries) >= 13
    task_sum_query = next(query for query in connection.queries if "WITH task_sum_runs AS" in query)
    assert "ti.task_id NOT ILIKE 'wait_for_%'" in task_sum_query
    assert "ti.task_id NOT ILIKE 'ge_test_%'" in task_sum_query
    assert "COALESCE(ti.operator_name, '') NOT ILIKE '%Sensor%'" in task_sum_query
    assert "ORDER BY dag_id" in task_sum_query

    markdown_text = markdown_path.read_text(encoding="utf-8")
    assert "# Monday DS Schedule Proposal" in markdown_text
    assert "## Reviewed Assumptions" in markdown_text
    assert "monday_ds_reviewed_assumptions.csv" in markdown_text
    assert "monday_ds_reviewed_assumptions.md" in markdown_text
    assert "monday_ds_why_each_dag_moved.md" in markdown_text
    assert "recipe_recommender" in markdown_text
    assert "30 10 * * 3" in markdown_text
    assert "Waiting time (" in markdown_text
    assert "Current run (" in markdown_text
    assert "Recent observed effective start UTC" in markdown_text
    assert "Proposed effective start UTC" in markdown_text
    assert "Modeled proposed effective finish UTC" in markdown_text
    assert "Representative current finish UTC" not in markdown_text
    assert "## Observed Airflow Limit Comparison" in markdown_text
    assert "monday_ds_observed_global_limits.csv" in markdown_text
    assert "monday_ds_observed_per_dag_limits.csv" in markdown_text
    assert "## Global Pressure Diagnostics" in markdown_text
    assert "## Pressure Evolution by UTC Hour" in markdown_text
    assert "### Estimated global pressure" in markdown_text
    assert "### Estimated DS parallel tasks" in markdown_text
    assert "| UTC hour | Before proposal | After proposal | Delta |" in markdown_text
    assert "Largest decrease at" in markdown_text
    assert "monday_ds_hourly_pressure_parallel.csv" in markdown_text
    assert "monday_ds_pressure_parallel_evolution.mmd" in markdown_text
    assert "monday_ds_global_pressure_evolution.mmd" in markdown_text
    assert "## Upstream Ready Diagnostics" not in markdown_text
    assert "## Runtime Estimation Comparison" not in markdown_text
    assert "## Representative Successful Runs" not in markdown_text

    assumptions_markdown_text = reviewed_assumptions_markdown_path.read_text(encoding="utf-8")
    assert "# Monday DS Reviewed Assumptions" in assumptions_markdown_text
    assert "Confidence guide:" in assumptions_markdown_text
    assert "reviewed_assumption" in assumptions_markdown_text

    why_each_dag_moved_text = why_each_dag_moved_markdown_path.read_text(encoding="utf-8")
    assert "# Monday DS Why Each DAG Moved" in why_each_dag_moved_text
    assert "recipe_recommender" in why_each_dag_moved_text
    assert "to remove 3h 25m of pre-ready waiting" in why_each_dag_moved_text
    assert "fixed multi-slot schedule" in why_each_dag_moved_text

    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        csv_rows = list(csv.DictReader(csv_file))

    assert [row["dag_id"] for row in csv_rows] == ["recipe_recommender", "menu_ranker"]
    assert csv_rows[0]["proposed_schedule"] == "30 10 * * 3"
    assert csv_rows[1]["strategy"] == "kept_existing_multi_slot_schedule"

    with hourly_pressure_csv_path.open(newline="", encoding="utf-8") as csv_file:
        hourly_pressure_rows = list(csv.DictReader(csv_file))

    with observed_global_limits_csv_path.open(newline="", encoding="utf-8") as csv_file:
        observed_global_limit_rows = list(csv.DictReader(csv_file))

    with observed_per_dag_limits_csv_path.open(newline="", encoding="utf-8") as csv_file:
        observed_per_dag_limit_rows = list(csv.DictReader(csv_file))

    assert len(hourly_pressure_rows) == 24
    assert set(hourly_pressure_rows[0].keys()) == {
        "hour",
        "global_avg_concurrency_current",
        "global_avg_concurrency_proposed",
        "global_peak_parallel_tasks_current",
        "global_peak_parallel_tasks_current_estimated",
        "global_peak_parallel_tasks_proposed",
        "global_peak_parallel_tasks_current_shifted_exact",
        "global_peak_parallel_tasks_proposed_shifted_exact",
        "ds_avg_concurrency_current",
        "ds_avg_concurrency_proposed",
        "ds_peak_parallel_tasks_current",
        "ds_peak_parallel_tasks_current_estimated",
        "ds_peak_parallel_tasks_proposed",
        "ds_peak_parallel_tasks_current_shifted_exact",
        "ds_peak_parallel_tasks_proposed_shifted_exact",
    }
    assert hourly_pressure_rows[0]["hour"] == "00:00"
    assert hourly_pressure_rows[10]["global_avg_concurrency_current"] == "5.0"
    assert hourly_pressure_rows[4]["global_peak_parallel_tasks_current"] == "24"
    assert hourly_pressure_rows[4]["global_peak_parallel_tasks_current_estimated"] == "20"
    assert hourly_pressure_rows[10]["global_peak_parallel_tasks_current"] == "22"
    assert hourly_pressure_rows[10]["global_peak_parallel_tasks_current_estimated"] == "16"
    assert hourly_pressure_rows[10]["global_peak_parallel_tasks_proposed"] == "16"
    assert hourly_pressure_rows[10]["global_peak_parallel_tasks_current_shifted_exact"] == "8"
    assert hourly_pressure_rows[10]["global_peak_parallel_tasks_proposed_shifted_exact"] == "1"
    assert hourly_pressure_rows[10]["ds_avg_concurrency_current"] == "0.23"
    assert hourly_pressure_rows[10]["ds_peak_parallel_tasks_current"] == "6"
    assert hourly_pressure_rows[10]["ds_peak_parallel_tasks_current_estimated"] == "0"
    assert hourly_pressure_rows[10]["ds_peak_parallel_tasks_proposed"] == "0"
    assert hourly_pressure_rows[10]["ds_peak_parallel_tasks_current_shifted_exact"] == "7"
    assert hourly_pressure_rows[10]["ds_peak_parallel_tasks_proposed_shifted_exact"] == "0"
    assert hourly_pressure_rows[13]["global_peak_parallel_tasks_proposed_shifted_exact"] == "7"
    assert hourly_pressure_rows[13]["ds_peak_parallel_tasks_proposed_shifted_exact"] == "7"

    assert observed_global_limit_rows == [
        {
            "metric": "global_running_tasks",
            "subject": "all_dags",
            "reference_limit_name": "parallelism",
            "reference_limit_value": "24",
            "observed_peak": "22",
            "peak_time": "2026-05-07 10:32:00+00:00",
            "within_limit": "True",
            "limit_headroom": "2",
            "limit_status": "below_limit",
        },
        {
            "metric": "scoped_running_tasks",
            "subject": "monday_ds",
            "reference_limit_name": "parallelism",
            "reference_limit_value": "24",
            "observed_peak": "6",
            "peak_time": "2026-05-07 10:35:00+00:00",
            "within_limit": "True",
            "limit_headroom": "18",
            "limit_status": "below_limit",
        },
    ]
    assert observed_per_dag_limit_rows == [
        {
            "dag_id": "menu_ranker",
            "configured_max_active_tasks_per_dag": "8",
            "observed_peak_running_tasks": "2",
            "running_tasks_peak_time": "2026-05-07 04:33:00+00:00",
            "within_max_active_tasks_per_dag": "True",
            "task_limit_headroom": "6",
            "task_limit_status": "below_limit",
            "configured_max_active_runs_per_dag": "1",
            "observed_peak_active_runs": "1",
            "active_runs_peak_time": "2026-05-07 04:30:00+00:00",
            "within_max_active_runs_per_dag": "True",
            "run_limit_headroom": "0",
            "run_limit_status": "at_limit",
        },
        {
            "dag_id": "recipe_recommender",
            "configured_max_active_tasks_per_dag": "8",
            "observed_peak_running_tasks": "7",
            "running_tasks_peak_time": "2026-05-07 10:40:00+00:00",
            "within_max_active_tasks_per_dag": "True",
            "task_limit_headroom": "1",
            "task_limit_status": "below_limit",
            "configured_max_active_runs_per_dag": "1",
            "observed_peak_active_runs": "1",
            "active_runs_peak_time": "2026-05-07 07:05:00+00:00",
            "within_max_active_runs_per_dag": "True",
            "run_limit_headroom": "0",
            "run_limit_status": "at_limit",
        },
    ]

    mermaid_chart_text = mermaid_chart_path.read_text(encoding="utf-8")
    assert mermaid_chart_text.startswith("---\nconfig:")
    assert "xychart" in mermaid_chart_text
    assert "Global Airflow Load by UTC Hour" in mermaid_chart_text
    assert mermaid_chart_text.count("line [") == 4

    global_mermaid_chart_text = global_mermaid_chart_path.read_text(encoding="utf-8")
    assert global_mermaid_chart_text.startswith("---\nconfig:")
    assert "Global Pressure by UTC Hour" in global_mermaid_chart_text
    assert global_mermaid_chart_text.count("line [") == 2