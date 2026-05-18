from __future__ import annotations

from dataclasses import asdict, dataclass


DEFAULT_TASK_SUM_EXCLUDED_TASK_PATTERNS = (
    "wait_for_%",
    "ge_test_%",
)

DEFAULT_TASK_SUM_EXCLUDED_OPERATOR_PATTERNS = (
    "%Sensor%",
    "%GreatExpectations%",
)


OptimizationInputRow = tuple[
    str,
    str,
    int | None,
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
]


@dataclass(frozen=True)
class WorkingHours:
    earliest_start_minute: int
    latest_start_minute: int


@dataclass(frozen=True)
class SlottedDagPlanInput:
    dag_id: str
    current_schedule: str
    current_primary_start_minute: int
    current_effective_start_minute: int
    effective_start_delay_minutes: int
    upstream_ready_minute: int
    dependency_gate_offset_minutes: int
    post_ready_setup_minutes: int
    schedule_suffix: str
    pressure_buffer_minutes: int
    direct_upstream_dependency_count: int
    avg_dag_runtime_seconds: float
    median_dag_runtime_seconds: float
    p90_dag_runtime_seconds: float
    median_schedule_to_end_seconds: float
    avg_effective_start_delay_seconds: float
    p90_effective_start_delay_seconds: float
    avg_effective_processing_seconds: float
    median_effective_processing_seconds: float
    p90_effective_processing_seconds: float
    total_scoped_idle_wait_seconds: float
    mapped_upstream_idle_wait_seconds: float
    mapped_edge_max_p90_idle_wait_seconds: float
    mapped_edge_max_avg_ready_seconds: float
    mapped_edge_max_median_clipped_ready_seconds: float
    mapped_edge_max_p90_ready_seconds: float
    mapped_edge_max_avg_sensor_touch_seconds: float
    mapped_edge_max_p90_sensor_touch_seconds: float
    effective_processing_minutes: int
    typical_processing_minutes: int
    median_task_count: float
    force_earliest_ready_slot: bool = False


@dataclass(frozen=True)
class ProposalRow:
    dag_id: str
    current_schedule: str
    proposed_schedule: str
    current_primary_start_utc: str
    proposed_primary_start_utc: str
    current_effective_start_utc: str
    proposed_effective_start_utc: str
    estimated_upstream_ready_utc: str
    current_wait_before_ready_minutes: int
    proposed_wait_before_ready_minutes: int
    current_gap_after_ready_minutes: int
    proposed_gap_after_ready_minutes: int
    wait_saved_minutes: int
    current_estimated_finish_utc: str
    proposed_estimated_finish_utc: str
    shift_minutes: int
    pressure_buffer_minutes: int
    effective_start_delay_minutes: int
    post_ready_setup_minutes: int
    direct_upstream_dependency_count: int
    avg_dag_runtime_seconds: float
    median_dag_runtime_seconds: float
    p90_dag_runtime_seconds: float
    avg_effective_start_delay_seconds: float
    p90_effective_start_delay_seconds: float
    avg_effective_processing_seconds: float
    median_effective_processing_seconds: float
    p90_effective_processing_seconds: float
    total_scoped_idle_wait_seconds: float
    mapped_upstream_idle_wait_seconds: float
    mapped_edge_max_p90_idle_wait_seconds: float
    mapped_edge_max_avg_ready_seconds: float
    mapped_edge_max_median_clipped_ready_seconds: float
    mapped_edge_max_p90_ready_seconds: float
    mapped_edge_max_avg_sensor_touch_seconds: float
    mapped_edge_max_p90_sensor_touch_seconds: float
    strategy: str
    recent_observed_effective_start_utc: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class RepresentativeRunRow:
    dag_id: str
    run_id: str
    logical_date: str
    start_delay_seconds: float | None
    dag_runtime_seconds: float | None
    schedule_to_end_seconds: float | None
    create_config_delay_seconds: float | None
    create_config_to_end_seconds: float | None


@dataclass(frozen=True)
class RepresentativeRunProfile:
    dag_id: str
    run_id: str
    logical_date: str
    anchor: str
    start_delay_seconds: float
    processing_seconds: float
    schedule_to_end_seconds: float


@dataclass(frozen=True)
class RuntimeEstimationConfig:
    default_strategy: str
    task_sum_excluded_task_patterns: tuple[str, ...]
    task_sum_excluded_operator_patterns: tuple[str, ...]
    manual_overrides_seconds: dict[str, float]
    dependency_gate_offsets_seconds: dict[str, float]


@dataclass(frozen=True)
class SchedulingSolverConfig:
    backend: str
    objective_mode: str
    parallelism_limit: int | None
    soft_parallelism_fraction: float
    time_limit_seconds: float
    sequential_dag_pairs: tuple[tuple[str, str], ...] = ()
    ready_start_dag_ids: tuple[str, ...] = ()
    dependency_gate_pairs: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class TaskSumEstimate:
    dag_id: str
    sample_count: int
    avg_task_sum_seconds: float
    median_task_sum_seconds: float
    p90_task_sum_seconds: float


@dataclass(frozen=True)
class ObservedPeak:
    subject: str
    observed_peak: int
    peak_time: str


@dataclass(frozen=True)
class SlottedDagAssignment:
    dag_id: str
    proposed_primary_start_minute: int
    proposed_effective_start_minute: int
    strategy: str


@dataclass(frozen=True)
class SchedulingSolveResult:
    assignments: list[SlottedDagAssignment]
    status: str
    rejection_reason: str | None = None


TaskCountEstimate = float
GlobalPressureProfile = dict[int, float]
