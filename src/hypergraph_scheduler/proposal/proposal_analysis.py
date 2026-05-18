from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta

from hypergraph_scheduler.scheduling.models import ProposalRow, RepresentativeRunProfile, RuntimeEstimationConfig, TaskSumEstimate
from hypergraph_scheduler.scheduling.runtime_estimation import proposal_effective_window_minutes
from hypergraph_scheduler.scheduling.slot_optimization import iter_window_buckets
from hypergraph_scheduler.scheduling.time_utils import parse_hhmm, round_down_to_bucket


def estimated_parallel_tasks(
    task_sum_estimate: TaskSumEstimate | None,
    processing_minutes: int,
) -> float:
    if task_sum_estimate is None or processing_minutes <= 0:
        return 0.0
    task_sum_seconds = task_sum_estimate.median_task_sum_seconds or task_sum_estimate.avg_task_sum_seconds
    if task_sum_seconds <= 0:
        return 0.0
    return round(task_sum_seconds / float(processing_minutes * 60), 2)


def build_scoped_parallel_task_series(
    proposal_rows: list[ProposalRow],
    representative_profiles: dict[str, RepresentativeRunProfile | None],
    runtime_estimation_config: RuntimeEstimationConfig,
    task_sum_estimates: dict[str, TaskSumEstimate],
    bucket_minutes: int,
) -> tuple[dict[int, float], dict[int, float]]:
    current_series: dict[int, float] = {}
    proposed_series: dict[int, float] = {}

    for proposal in proposal_rows:
        profile = representative_profiles.get(proposal.dag_id)
        current_effective_minutes, proposed_effective_minutes, processing_minutes = proposal_effective_window_minutes(
            proposal,
            profile,
            runtime_estimation_config,
        )
        parallel_tasks = estimated_parallel_tasks(task_sum_estimates.get(proposal.dag_id), processing_minutes)
        if parallel_tasks <= 0:
            continue
        for bucket_minute in iter_window_buckets(
            current_effective_minutes,
            current_effective_minutes + processing_minutes,
            bucket_minutes,
        ):
            minute_of_day = bucket_minute % (24 * 60)
            current_series[minute_of_day] = current_series.get(minute_of_day, 0.0) + parallel_tasks
        for bucket_minute in iter_window_buckets(
            proposed_effective_minutes,
            proposed_effective_minutes + processing_minutes,
            bucket_minutes,
        ):
            minute_of_day = bucket_minute % (24 * 60)
            proposed_series[minute_of_day] = proposed_series.get(minute_of_day, 0.0) + parallel_tasks

    return current_series, proposed_series


def build_scoped_peak_task_series(
    proposal_rows: list[ProposalRow],
    historical_profiles_by_day: dict[str, dict[str, RepresentativeRunProfile]],
    task_intervals_by_run: dict[tuple[str, str], list[tuple[datetime, datetime]]],
    bucket_minutes: int,
) -> tuple[dict[int, float], dict[int, float]]:
    proposal_by_dag = {proposal.dag_id: proposal for proposal in proposal_rows}
    current_series: dict[int, float] = defaultdict(float)
    proposed_series: dict[int, float] = defaultdict(float)

    for profiles_by_dag in historical_profiles_by_day.values():
        current_day_series: dict[int, float] = defaultdict(float)
        proposed_day_series: dict[int, float] = defaultdict(float)

        for dag_id, profile in profiles_by_dag.items():
            proposal = proposal_by_dag.get(dag_id)
            if proposal is None:
                continue
            intervals = task_intervals_by_run.get((profile.dag_id, profile.run_id))
            if not intervals:
                continue

            logical_date = datetime.fromisoformat(profile.logical_date.replace("Z", "+00:00"))
            current_effective_minutes = parse_hhmm(proposal.current_effective_start_utc)
            proposed_effective_minutes = parse_hhmm(proposal.proposed_effective_start_utc)
            current_bucket_anchor = round_down_to_bucket(current_effective_minutes, bucket_minutes)
            proposed_bucket_anchor = round_down_to_bucket(proposed_effective_minutes, bucket_minutes)
            representative_anchor_dt = logical_date + timedelta(seconds=profile.start_delay_seconds)
            representative_anchor_bucket = round_down_to_bucket(
                representative_anchor_dt.hour * 60 + representative_anchor_dt.minute,
                bucket_minutes,
            )
            anchor_dt = representative_anchor_dt.replace(second=0, microsecond=0)
            anchor_dt = anchor_dt.replace(
                hour=representative_anchor_bucket // 60,
                minute=representative_anchor_bucket % 60,
            )
            anchor_epoch_minutes = int(anchor_dt.timestamp() // 60)
            offset_counts: dict[int, int] = defaultdict(int)

            for start_dt, end_dt in intervals:
                start_epoch_minutes = int(start_dt.timestamp() // 60)
                end_epoch_minutes = int((end_dt - timedelta(seconds=1)).timestamp() // 60)
                bucket_start = round_down_to_bucket(start_epoch_minutes, bucket_minutes)
                bucket_end = round_down_to_bucket(max(end_epoch_minutes, start_epoch_minutes), bucket_minutes)
                for bucket_epoch_minutes in range(bucket_start, bucket_end + bucket_minutes, bucket_minutes):
                    offset_minutes = bucket_epoch_minutes - anchor_epoch_minutes
                    if offset_minutes < 0:
                        continue
                    offset_counts[offset_minutes] += 1

            for offset_minutes, peak_count in offset_counts.items():
                current_minute_of_day = (current_bucket_anchor + offset_minutes) % (24 * 60)
                proposed_minute_of_day = (proposed_bucket_anchor + offset_minutes) % (24 * 60)
                current_day_series[current_minute_of_day] += float(peak_count)
                proposed_day_series[proposed_minute_of_day] += float(peak_count)

        for minute_of_day, peak_count in current_day_series.items():
            current_series[minute_of_day] = max(current_series.get(minute_of_day, 0.0), peak_count)
        for minute_of_day, peak_count in proposed_day_series.items():
            proposed_series[minute_of_day] = max(proposed_series.get(minute_of_day, 0.0), peak_count)

    return dict(current_series), dict(proposed_series)


def build_exact_shifted_peak_task_series(
    proposal_rows: list[ProposalRow],
    scoped_task_intervals_by_dag: dict[str, list[tuple[datetime, datetime]]],
    all_task_intervals_by_dag: dict[str, list[tuple[datetime, datetime]]],
    bucket_minutes: int,
) -> tuple[dict[int, float], dict[int, float], dict[int, float], dict[int, float]]:
    shift_minutes_by_dag = {
        proposal.dag_id: parse_hhmm(proposal.proposed_primary_start_utc) - parse_hhmm(proposal.current_primary_start_utc)
        for proposal in proposal_rows
    }
    scoped_dag_ids = set(scoped_task_intervals_by_dag)
    current_scoped_events: dict[str, list[tuple[datetime, int]]] = defaultdict(list)
    proposed_scoped_events: dict[str, list[tuple[datetime, int]]] = defaultdict(list)
    current_global_events: dict[str, list[tuple[datetime, int]]] = defaultdict(list)
    proposed_global_events: dict[str, list[tuple[datetime, int]]] = defaultdict(list)

    def add_events(storage: dict[str, list[tuple[datetime, int]]], start_dt: datetime, end_dt: datetime) -> None:
        date_key = start_dt.date().isoformat()
        storage[date_key].append((start_dt, 1))
        storage[date_key].append((end_dt, -1))

    for dag_id, intervals in all_task_intervals_by_dag.items():
        shift_minutes = shift_minutes_by_dag.get(dag_id, 0)
        is_scoped = dag_id in scoped_dag_ids
        for start_dt, end_dt in intervals:
            add_events(current_global_events, start_dt, end_dt)
            shifted_start = start_dt + timedelta(minutes=shift_minutes)
            shifted_end = end_dt + timedelta(minutes=shift_minutes)
            add_events(proposed_global_events, shifted_start, shifted_end)
            if not is_scoped:
                continue
            add_events(current_scoped_events, start_dt, end_dt)
            add_events(proposed_scoped_events, shifted_start, shifted_end)

    def peak_by_hour(events_by_day: dict[str, list[tuple[datetime, int]]]) -> dict[int, float]:
        result: dict[int, float] = defaultdict(float)
        for events in events_by_day.values():
            sorted_events = sorted(events, key=lambda item: (item[0], -item[1]))
            active_count = 0
            previous_time: datetime | None = None
            for event_time, delta in sorted_events:
                if previous_time is not None and event_time > previous_time and active_count > 0:
                    hour_cursor = previous_time.replace(minute=0, second=0, microsecond=0)
                    if hour_cursor < previous_time:
                        hour_cursor = hour_cursor
                    while hour_cursor < event_time:
                        minute_of_day = hour_cursor.hour * 60
                        result[minute_of_day] = max(result.get(minute_of_day, 0.0), float(active_count))
                        hour_cursor += timedelta(hours=1)
                active_count += delta
                previous_time = event_time
        return dict(result)

    return (
        peak_by_hour(current_scoped_events),
        peak_by_hour(proposed_scoped_events),
        peak_by_hour(current_global_events),
        peak_by_hour(proposed_global_events),
    )