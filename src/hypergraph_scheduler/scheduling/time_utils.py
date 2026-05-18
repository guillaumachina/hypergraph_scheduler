from __future__ import annotations

import math


def parse_hhmm(value: str) -> int:
    hour_str, minute_str = value.split(":", maxsplit=1)
    return int(hour_str) * 60 + int(minute_str)


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
