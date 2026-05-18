from __future__ import annotations

import json
from pathlib import Path
from statistics import median

from hypergraph_scheduler.scheduling.models import (
    DEFAULT_TASK_SUM_EXCLUDED_OPERATOR_PATTERNS,
    DEFAULT_TASK_SUM_EXCLUDED_TASK_PATTERNS,
    ProposalRow,
    RepresentativeRunProfile,
    RepresentativeRunRow,
    RuntimeEstimationConfig,
    SchedulingSolverConfig,
    WorkingHours,
)
from hypergraph_scheduler.scheduling.time_utils import parse_hhmm


def load_optimization_model(model_path: Path) -> dict[str, object]:
    return json.loads(model_path.read_text(encoding="utf-8"))


def load_working_hours(model_path: Path) -> WorkingHours:
    model = load_optimization_model(model_path)
    optimization_defaults = model.get("optimization_defaults")
    if not isinstance(optimization_defaults, dict):
        raise ValueError(f"Invalid optimization model defaults in {model_path}")
    working_hours = optimization_defaults.get("working_hours_constraint")
    if not isinstance(working_hours, dict):
        raise ValueError(f"Missing working_hours_constraint in {model_path}")
    return WorkingHours(
        earliest_start_minute=parse_hhmm(str(working_hours["earliest_start"])),
        latest_start_minute=parse_hhmm(str(working_hours["latest_start"])),
    )


def load_runtime_estimation_config(model_path: Path) -> RuntimeEstimationConfig:
    model = load_optimization_model(model_path)
    optimization_defaults = model.get("optimization_defaults", {})
    if not isinstance(optimization_defaults, dict):
        optimization_defaults = {}
    runtime_estimation = optimization_defaults.get("runtime_estimation", {})
    if not isinstance(runtime_estimation, dict):
        runtime_estimation = {}
    dags = model.get("dags", [])
    if not isinstance(dags, list):
        dags = []

    manual_overrides_seconds = {
        str(dag_entry["dag_id"]): float(runtime_settings["manual_estimated_seconds"])
        for dag_entry in dags
        if isinstance(dag_entry, dict)
        for runtime_settings in [dag_entry.get("runtime_estimation", {})]
        if isinstance(runtime_settings, dict) and runtime_settings.get("manual_estimated_seconds") is not None
    }
    dependency_gate_offsets_seconds = {
        str(dag_entry["dag_id"]): float(runtime_settings["dependency_gate_offset_seconds"])
        for dag_entry in dags
        if isinstance(dag_entry, dict)
        for runtime_settings in [dag_entry.get("runtime_estimation", {})]
        if isinstance(runtime_settings, dict) and runtime_settings.get("dependency_gate_offset_seconds") is not None
    }

    return RuntimeEstimationConfig(
        default_strategy=str(runtime_estimation.get("default_strategy", "aggregate_create_config")),
        task_sum_excluded_task_patterns=tuple(
            runtime_estimation.get("task_sum_excluded_task_patterns", DEFAULT_TASK_SUM_EXCLUDED_TASK_PATTERNS)
        ),
        task_sum_excluded_operator_patterns=tuple(
            runtime_estimation.get(
                "task_sum_excluded_operator_patterns",
                DEFAULT_TASK_SUM_EXCLUDED_OPERATOR_PATTERNS,
            )
        ),
        manual_overrides_seconds=manual_overrides_seconds,
        dependency_gate_offsets_seconds=dependency_gate_offsets_seconds,
    )


def load_solver_config(
    model_path: Path,
    backend_override: str | None = None,
    objective_mode_override: str | None = None,
) -> SchedulingSolverConfig:
    model = load_optimization_model(model_path)
    optimization_defaults = model.get("optimization_defaults", {})
    if not isinstance(optimization_defaults, dict):
        optimization_defaults = {}
    solver_config = optimization_defaults.get("solver", {})
    if not isinstance(solver_config, dict):
        solver_config = {}

    raw_parallelism_limit = solver_config.get("parallelism_limit", 24)
    parallelism_limit = int(raw_parallelism_limit) if raw_parallelism_limit is not None else None
    backend = str(backend_override or solver_config.get("default_backend", "greedy")).strip().lower()
    if backend not in {"greedy", "cp_sat", "milp"}:
        raise ValueError(f"Unsupported solver backend '{backend}' in {model_path}")
    objective_mode = str(objective_mode_override or solver_config.get("objective_mode", "wait_saving")).strip().lower()
    if objective_mode not in {"wait_saving", "concurrency_first"}:
        raise ValueError(f"Unsupported solver objective_mode '{objective_mode}' in {model_path}")
    raw_sequential_dag_pairs = solver_config.get("sequential_dag_pairs", [])
    sequential_dag_pairs: list[tuple[str, str]] = []
    if isinstance(raw_sequential_dag_pairs, list):
        for entry in raw_sequential_dag_pairs:
            if not isinstance(entry, dict):
                continue
            primary_dag_id = entry.get("primary_dag_id")
            secondary_dag_id = entry.get("secondary_dag_id")
            if primary_dag_id and secondary_dag_id:
                sequential_dag_pairs.append((str(primary_dag_id), str(secondary_dag_id)))
    raw_ready_start_dag_ids = solver_config.get("ready_start_dag_ids", [])
    ready_start_dag_ids: list[str] = []
    if isinstance(raw_ready_start_dag_ids, list):
        ready_start_dag_ids = [str(dag_id) for dag_id in raw_ready_start_dag_ids if dag_id]
    raw_dependency_gate_pairs = solver_config.get("dependency_gate_pairs", [])
    dependency_gate_pairs: list[tuple[str, str]] = []
    if isinstance(raw_dependency_gate_pairs, list):
        for entry in raw_dependency_gate_pairs:
            if not isinstance(entry, dict):
                continue
            upstream_dag_id = entry.get("upstream_dag_id")
            gated_dag_id = entry.get("gated_dag_id")
            if upstream_dag_id and gated_dag_id:
                dependency_gate_pairs.append((str(upstream_dag_id), str(gated_dag_id)))

    return SchedulingSolverConfig(
        backend=backend,
        objective_mode=objective_mode,
        parallelism_limit=parallelism_limit,
        soft_parallelism_fraction=float(solver_config.get("soft_parallelism_fraction", 0.75)),
        time_limit_seconds=float(solver_config.get("time_limit_seconds", 10.0)),
        sequential_dag_pairs=tuple(sequential_dag_pairs),
        ready_start_dag_ids=tuple(ready_start_dag_ids),
        dependency_gate_pairs=tuple(dependency_gate_pairs),
    )


def choose_typical_runtime_seconds(avg_runtime_seconds: float | None, median_runtime_seconds: float | None) -> float:
    if median_runtime_seconds is None:
        return avg_runtime_seconds or 0.0
    if avg_runtime_seconds is None or avg_runtime_seconds <= 0:
        return median_runtime_seconds
    if 0.5 * avg_runtime_seconds <= median_runtime_seconds <= 1.5 * avg_runtime_seconds:
        return median_runtime_seconds
    return avg_runtime_seconds


def choose_recommender_processing_seconds(
    *,
    manual_override_seconds: float | None,
    median_effective_processing_seconds: float | None,
    avg_effective_processing_seconds: float | None,
    avg_dag_runtime_seconds: float | None,
    median_dag_runtime_seconds: float | None,
) -> float:
    if manual_override_seconds is not None and manual_override_seconds > 0:
        return manual_override_seconds
    if median_effective_processing_seconds is not None and median_effective_processing_seconds > 0:
        return median_effective_processing_seconds
    if avg_effective_processing_seconds is not None and avg_effective_processing_seconds > 0:
        return avg_effective_processing_seconds
    return choose_typical_runtime_seconds(avg_dag_runtime_seconds, median_dag_runtime_seconds)


def build_replay_profile(run_row: RepresentativeRunRow) -> RepresentativeRunProfile | None:
    if (
        run_row.create_config_delay_seconds is not None
        and run_row.create_config_to_end_seconds is not None
        and 5 * 60 <= run_row.create_config_to_end_seconds <= 12 * 60 * 60
        and run_row.create_config_delay_seconds <= 8 * 60 * 60
        and run_row.schedule_to_end_seconds is not None
    ):
        return RepresentativeRunProfile(
            dag_id=run_row.dag_id,
            run_id=run_row.run_id,
            logical_date=run_row.logical_date,
            anchor="create_config",
            start_delay_seconds=run_row.create_config_delay_seconds or 0.0,
            processing_seconds=run_row.create_config_to_end_seconds or 0.0,
            schedule_to_end_seconds=run_row.schedule_to_end_seconds or 0.0,
        )

    if (
        run_row.dag_runtime_seconds is not None
        and 15 * 60 <= run_row.dag_runtime_seconds <= 6 * 60 * 60
        and run_row.schedule_to_end_seconds is not None
    ):
        return RepresentativeRunProfile(
            dag_id=run_row.dag_id,
            run_id=run_row.run_id,
            logical_date=run_row.logical_date,
            anchor="dag_run",
            start_delay_seconds=run_row.start_delay_seconds or 0.0,
            processing_seconds=run_row.dag_runtime_seconds or 0.0,
            schedule_to_end_seconds=run_row.schedule_to_end_seconds or 0.0,
        )

    if run_row.schedule_to_end_seconds is None:
        return None

    return RepresentativeRunProfile(
        dag_id=run_row.dag_id,
        run_id=run_row.run_id,
        logical_date=run_row.logical_date,
        anchor="fallback_schedule_to_end",
        start_delay_seconds=run_row.create_config_delay_seconds or run_row.start_delay_seconds or 0.0,
        processing_seconds=run_row.create_config_to_end_seconds or run_row.dag_runtime_seconds or 0.0,
        schedule_to_end_seconds=run_row.schedule_to_end_seconds or 0.0,
    )


def choose_representative_run(run_rows: list[RepresentativeRunRow]) -> RepresentativeRunProfile | None:
    if not run_rows:
        return None

    create_config_candidates = [
        row
        for row in run_rows
        if row.create_config_delay_seconds is not None
        and row.create_config_to_end_seconds is not None
        and 5 * 60 <= row.create_config_to_end_seconds <= 12 * 60 * 60
        and row.create_config_delay_seconds <= 8 * 60 * 60
        and row.schedule_to_end_seconds is not None
    ]
    if create_config_candidates:
        target_processing_seconds = median(
            row.create_config_to_end_seconds
            for row in create_config_candidates
            if row.create_config_to_end_seconds is not None
        )
        chosen = min(
            create_config_candidates,
            key=lambda row: (
                abs((row.create_config_to_end_seconds or 0.0) - target_processing_seconds),
                row.create_config_to_end_seconds or 0.0,
                row.logical_date,
            ),
        )
        return RepresentativeRunProfile(
            dag_id=chosen.dag_id,
            run_id=chosen.run_id,
            logical_date=chosen.logical_date,
            anchor="create_config",
            start_delay_seconds=chosen.create_config_delay_seconds or 0.0,
            processing_seconds=chosen.create_config_to_end_seconds or 0.0,
            schedule_to_end_seconds=chosen.schedule_to_end_seconds or 0.0,
        )

    dag_runtime_candidates = [
        row
        for row in run_rows
        if row.dag_runtime_seconds is not None
        and 15 * 60 <= row.dag_runtime_seconds <= 6 * 60 * 60
        and row.schedule_to_end_seconds is not None
    ]
    if dag_runtime_candidates:
        target_runtime_seconds = median(
            row.dag_runtime_seconds for row in dag_runtime_candidates if row.dag_runtime_seconds is not None
        )
        chosen = min(
            dag_runtime_candidates,
            key=lambda row: (
                abs((row.dag_runtime_seconds or 0.0) - target_runtime_seconds),
                row.dag_runtime_seconds or 0.0,
                row.logical_date,
            ),
        )
        return RepresentativeRunProfile(
            dag_id=chosen.dag_id,
            run_id=chosen.run_id,
            logical_date=chosen.logical_date,
            anchor="dag_run",
            start_delay_seconds=chosen.start_delay_seconds or 0.0,
            processing_seconds=chosen.dag_runtime_seconds or 0.0,
            schedule_to_end_seconds=chosen.dag_runtime_seconds or 0.0,
        )

    chosen = min(run_rows, key=lambda row: row.schedule_to_end_seconds or float("inf"))
    return RepresentativeRunProfile(
        dag_id=chosen.dag_id,
        run_id=chosen.run_id,
        logical_date=chosen.logical_date,
        anchor="fallback_schedule_to_end",
        start_delay_seconds=chosen.create_config_delay_seconds or chosen.start_delay_seconds or 0.0,
        processing_seconds=chosen.create_config_to_end_seconds or chosen.dag_runtime_seconds or 0.0,
        schedule_to_end_seconds=chosen.schedule_to_end_seconds or 0.0,
    )


def profile_start_delay_minutes(profile: RepresentativeRunProfile | None, fallback_minutes: int) -> int:
    if profile is None:
        return fallback_minutes
    return int(round(profile.start_delay_seconds / 60.0))


def profile_processing_minutes(profile: RepresentativeRunProfile | None, fallback_minutes: int) -> int:
    if profile is None:
        return fallback_minutes
    return int(round(profile.processing_seconds / 60.0))


def profile_completion_minutes(profile: RepresentativeRunProfile | None, fallback_minutes: int) -> int:
    if profile is None:
        return fallback_minutes
    return int(round(profile.schedule_to_end_seconds / 60.0))


def proposal_effective_window_minutes(
    proposal: ProposalRow,
    profile: RepresentativeRunProfile | None,
    runtime_estimation_config: RuntimeEstimationConfig,
) -> tuple[int, int, int]:
    processing_minutes = int(
        round(
            choose_recommender_processing_seconds(
                manual_override_seconds=runtime_estimation_config.manual_overrides_seconds.get(proposal.dag_id),
                median_effective_processing_seconds=proposal.median_effective_processing_seconds,
                avg_effective_processing_seconds=proposal.avg_effective_processing_seconds,
                avg_dag_runtime_seconds=proposal.avg_dag_runtime_seconds,
                median_dag_runtime_seconds=proposal.median_dag_runtime_seconds,
            )
            / 60.0
        )
    )
    current_effective_minutes = parse_hhmm(proposal.current_effective_start_utc)
    proposed_effective_minutes = parse_hhmm(proposal.proposed_effective_start_utc)
    return current_effective_minutes, proposed_effective_minutes, processing_minutes
