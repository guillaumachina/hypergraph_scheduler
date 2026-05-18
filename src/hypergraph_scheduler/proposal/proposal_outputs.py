from __future__ import annotations

import math


def hourly_average_series(values_by_minute: dict[int, float], bucket_minutes: int) -> list[float]:
    hourly_values: list[float] = []
    for hour in range(24):
        hour_start = hour * 60
        bucket_values = [
            values_by_minute.get(minute_of_day, 0.0)
            for minute_of_day in range(hour_start, hour_start + 60, bucket_minutes)
        ]
        if not bucket_values:
            hourly_values.append(0.0)
            continue
        hourly_values.append(round(sum(bucket_values) / len(bucket_values), 2))
    return hourly_values


def hourly_peak_series(values_by_minute: dict[int, float], bucket_minutes: int) -> list[float]:
    hourly_values: list[float] = []
    for hour in range(24):
        hour_start = hour * 60
        bucket_values = [
            values_by_minute.get(minute_of_day, 0.0)
            for minute_of_day in range(hour_start, hour_start + 60, bucket_minutes)
        ]
        hourly_values.append(round(max(bucket_values, default=0.0), 2))
    return hourly_values


def hourly_peak_slot_series(values_by_minute: dict[int, float], bucket_minutes: int) -> list[int]:
    hourly_values: list[int] = []
    for hour in range(24):
        hour_start = hour * 60
        bucket_values = [
            values_by_minute.get(minute_of_day, 0.0)
            for minute_of_day in range(hour_start, hour_start + 60, bucket_minutes)
        ]
        hourly_values.append(int(math.ceil(max(bucket_values, default=0.0))))
    return hourly_values


def bucket_series(values_by_minute: dict[int, float], bucket_minutes: int) -> list[float]:
    return [
        round(values_by_minute.get(minute_of_day, 0.0), 2)
        for minute_of_day in range(0, 24 * 60, bucket_minutes)
    ]


def averaged_bucket_series(
    values_by_minute: dict[int, float],
    source_bucket_minutes: int,
    target_bucket_minutes: int,
) -> list[float]:
    if target_bucket_minutes % source_bucket_minutes != 0:
        raise ValueError("target bucket must be a multiple of source bucket")
    values: list[float] = []
    for window_start in range(0, 24 * 60, target_bucket_minutes):
        bucket_values = [
            values_by_minute.get(minute_of_day, 0.0)
            for minute_of_day in range(window_start, window_start + target_bucket_minutes, source_bucket_minutes)
        ]
        values.append(round(sum(bucket_values) / len(bucket_values), 2) if bucket_values else 0.0)
    return values


def chart_y_axis_max(series_list: list[list[float]]) -> int:
    max_value = max((max(series, default=0.0) for series in series_list), default=0.0)
    if max_value <= 0:
        return 1
    return int(math.ceil(max_value / 5.0) * 5)


def append_hourly_table(
    lines: list[str],
    title: str,
    before_label: str,
    before_series: list[float],
    after_label: str,
    after_series: list[float],
) -> None:
    lines.extend(
        [
            f"### {title}",
            "",
            f"| UTC hour | {before_label} | {after_label} | Delta |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for hour, (before_value, after_value) in enumerate(zip(before_series, after_series, strict=False)):
        lines.append(
            "| {:02d}:00 | {:.2f} | {:.2f} | {:+.2f} |".format(
                hour,
                before_value,
                after_value,
                after_value - before_value,
            )
        )


def append_hourly_delta_summary(
    lines: list[str],
    before_series: list[float],
    after_series: list[float],
) -> None:
    deltas = [after - before for before, after in zip(before_series, after_series, strict=False)]
    min_delta = min(deltas, default=0.0)
    max_delta = max(deltas, default=0.0)
    min_hour = deltas.index(min_delta) if deltas else 0
    max_hour = deltas.index(max_delta) if deltas else 0
    lines.extend(
        [
            "",
            "Delta summary:",
            f"Largest decrease at {min_hour:02d}:00: {min_delta:+.2f}",
            f"Largest increase at {max_hour:02d}:00: {max_delta:+.2f}",
        ]
    )


def build_combined_hourly_xychart(
    title: str,
    x_axis_title: str,
    x_axis_labels: list[str],
    current_global_pressure: list[float],
    proposed_global_pressure: list[float],
    current_parallel_tasks: list[float],
    proposed_parallel_tasks: list[float],
) -> str:
    x_axis_values = ", ".join(f'"{label}"' for label in x_axis_labels)

    def format_series(values: list[float]) -> str:
        return ", ".join(f"{value:.2f}" for value in values)

    y_axis_max = chart_y_axis_max(
        [
            current_global_pressure,
            proposed_global_pressure,
            current_parallel_tasks,
            proposed_parallel_tasks,
        ]
    )
    return "\n".join(
        [
            "---",
            "config:",
            "  themeVariables:",
            "    xyChart:",
            "      plotColorPalette: '#1f77b4, #d62728, #2ca02c, #ff7f0e'",
            "---",
            "xychart",
            f"    title \"{title}\"",
            f"    x-axis \"{x_axis_title}\" [{x_axis_values}]",
            f"    y-axis \"Global task count\" 0 --> {y_axis_max}",
            "    %% Line 1: hourly average global pressure before proposal",
            f"    line [{format_series(current_global_pressure)}]",
            "    %% Line 2: hourly average global pressure after proposal",
            f"    line [{format_series(proposed_global_pressure)}]",
            "    %% Line 3: hourly peak global parallel tasks before proposal",
            f"    line [{format_series(current_parallel_tasks)}]",
            "    %% Line 4: hourly peak global parallel tasks after proposal",
            f"    line [{format_series(proposed_parallel_tasks)}]",
        ]
    )


def build_global_pressure_xychart(
    title: str,
    x_axis_title: str,
    x_axis_labels: list[str],
    current_global_pressure: list[float],
    proposed_global_pressure: list[float],
) -> str:
    x_axis_values = ", ".join(f'"{label}"' for label in x_axis_labels)

    def format_series(values: list[float]) -> str:
        return ", ".join(f"{value:.2f}" for value in values)

    y_axis_max = chart_y_axis_max([current_global_pressure, proposed_global_pressure])
    return "\n".join(
        [
            "---",
            "config:",
            "  themeVariables:",
            "    xyChart:",
            "      plotColorPalette: '#1f77b4, #d62728'",
            "---",
            "xychart",
            f"    title \"{title}\"",
            f"    x-axis \"{x_axis_title}\" [{x_axis_values}]",
            f"    y-axis \"Global running tasks\" 0 --> {y_axis_max}",
            "    %% Line 1: current global pressure / median global running tasks",
            f"    line [{format_series(current_global_pressure)}]",
            "    %% Line 2: proposed global pressure / estimated median global running tasks",
            f"    line [{format_series(proposed_global_pressure)}]",
        ]
    )
