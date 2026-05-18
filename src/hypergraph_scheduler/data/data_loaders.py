from __future__ import annotations

from collections import defaultdict
from datetime import datetime

import duckdb

from hypergraph_scheduler.scheduling.models import (
    GlobalPressureProfile,
    ObservedPeak,
    RepresentativeRunProfile,
    RuntimeEstimationConfig,
    TaskCountEstimate,
    TaskSumEstimate,
)


def coerce_float(value: object) -> float | None:
    if value is None:
        return None
    if not isinstance(value, (float, int, str)):
        return None
    return float(value)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def coerce_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def load_task_sum_estimates(
    connection: duckdb.DuckDBPyConnection,
    dag_ids: set[str],
    config: RuntimeEstimationConfig,
) -> dict[str, TaskSumEstimate]:
    if not dag_ids:
        return {}

    task_exclusion_clauses = "".join(
        f"\n          AND ti.task_id NOT ILIKE {sql_literal(pattern)}"
        for pattern in config.task_sum_excluded_task_patterns
    )
    operator_exclusion_clauses = "".join(
        f"\n          AND COALESCE(ti.operator_name, '') NOT ILIKE {sql_literal(pattern)}"
        for pattern in config.task_sum_excluded_operator_patterns
    )
    dag_id_list = ", ".join(sql_literal(dag_id) for dag_id in sorted(dag_ids))

    rows = connection.execute(
        f"""
        WITH task_sum_runs AS (
            SELECT
                ti.dag_id,
                ti.run_id,
                SUM(ti.task_elapsed_seconds) AS total_task_sum_seconds
            FROM task_instances_enriched ti
            WHERE ti.dag_id IN ({dag_id_list})
              AND ti.state = 'success'
              AND ti.task_elapsed_seconds IS NOT NULL
              AND ti.start_date IS NOT NULL
              AND ti.end_date IS NOT NULL{task_exclusion_clauses}{operator_exclusion_clauses}
            GROUP BY ti.dag_id, ti.run_id
        )
        SELECT
            dag_id,
            COUNT(*) AS run_count,
            AVG(total_task_sum_seconds) AS avg_task_sum_seconds,
            MEDIAN(total_task_sum_seconds) AS median_task_sum_seconds,
            QUANTILE_CONT(total_task_sum_seconds, 0.9) AS p90_task_sum_seconds
        FROM task_sum_runs
        GROUP BY dag_id
        ORDER BY dag_id
        """
    ).fetchall()

    estimates: dict[str, TaskSumEstimate] = {}
    for row in rows:
        dag_id = str(row[0])
        estimates[dag_id] = TaskSumEstimate(
            dag_id=dag_id,
            sample_count=int(row[1]),
            avg_task_sum_seconds=coerce_float(row[2]) or 0.0,
            median_task_sum_seconds=coerce_float(row[3]) or 0.0,
            p90_task_sum_seconds=coerce_float(row[4]) or 0.0,
        )
    return estimates


def load_task_count_estimates(
    connection: duckdb.DuckDBPyConnection,
    dag_ids: set[str],
) -> dict[str, TaskCountEstimate]:
    if not dag_ids:
        return {}

    dag_id_list = ", ".join(sql_literal(dag_id) for dag_id in sorted(dag_ids))
    rows = connection.execute(
        f"""
        WITH per_run AS (
            SELECT
                ti.dag_id,
                ti.run_id,
                COUNT(*) AS task_count
            FROM task_instances_enriched ti
            WHERE ti.dag_id IN ({dag_id_list})
            GROUP BY ti.dag_id, ti.run_id
        )
        SELECT
            dag_id,
            MEDIAN(task_count) AS median_task_count
        FROM per_run
        GROUP BY dag_id
        ORDER BY dag_id
        """
    ).fetchall()

    return {str(row[0]): float(row[1]) for row in rows}


def load_global_pressure_profile(
    connection: duckdb.DuckDBPyConnection,
    bucket_minutes: int,
) -> GlobalPressureProfile:
    rows = connection.execute(
        f"""
        WITH task_buckets AS (
            SELECT
                gs.minute_bucket
            FROM task_instances_enriched ti,
            LATERAL generate_series(
                date_trunc('minute', ti.start_date AT TIME ZONE 'UTC'),
                date_trunc('minute', (ti.end_date AT TIME ZONE 'UTC') - INTERVAL '1 second'),
                INTERVAL '{bucket_minutes} minute'
            ) AS gs(minute_bucket)
            WHERE ti.start_date IS NOT NULL
              AND ti.end_date IS NOT NULL
              AND ti.end_date > ti.start_date
        ),
        bucket_counts AS (
            SELECT
                CAST(strftime(minute_bucket, '%H') AS INTEGER) * 60
                    + CAST(strftime(minute_bucket, '%M') AS INTEGER) AS minute_of_day,
                CAST(date(minute_bucket) AS VARCHAR) AS bucket_date,
                COUNT(*) AS running_task_count
            FROM task_buckets
            GROUP BY minute_of_day, bucket_date
        )
        SELECT
            minute_of_day,
            MEDIAN(running_task_count) AS median_running_task_count
        FROM bucket_counts
        GROUP BY minute_of_day
        ORDER BY minute_of_day
        """
    ).fetchall()

    return {int(row[0]): float(row[1]) for row in rows}


def load_observed_task_peak_profile(
    connection: duckdb.DuckDBPyConnection,
    bucket_minutes: int,
    dag_ids: set[str] | None = None,
    exclude_dag_ids: set[str] | None = None,
) -> GlobalPressureProfile:
    if dag_ids is not None and exclude_dag_ids is not None:
        raise ValueError("dag_ids and exclude_dag_ids cannot both be set")

    dag_filter = ""
    query_tag = "OBSERVED_GLOBAL_TASK_PEAK_PROFILE"
    if dag_ids is not None:
        if not dag_ids:
            return {}
        dag_id_list = ", ".join(sql_literal(dag_id) for dag_id in sorted(dag_ids))
        dag_filter = f"\n              AND ti.dag_id IN ({dag_id_list})"
        query_tag = "OBSERVED_SCOPED_TASK_PEAK_PROFILE"
    elif exclude_dag_ids is not None:
        dag_id_list = ", ".join(sql_literal(dag_id) for dag_id in sorted(exclude_dag_ids))
        dag_filter = f"\n              AND ti.dag_id NOT IN ({dag_id_list})"
        query_tag = "OBSERVED_NON_SCOPED_TASK_PEAK_PROFILE"

    rows = connection.execute(
        f"""
        -- {query_tag}
        WITH task_buckets AS (
            SELECT
                gs.minute_bucket
            FROM task_instances_enriched ti,
            LATERAL generate_series(
                date_trunc('minute', ti.start_date AT TIME ZONE 'UTC'),
                date_trunc('minute', (ti.end_date AT TIME ZONE 'UTC') - INTERVAL '1 second'),
                INTERVAL '{bucket_minutes} minute'
            ) AS gs(minute_bucket)
            WHERE ti.start_date IS NOT NULL
              AND ti.end_date IS NOT NULL
              AND ti.end_date > ti.start_date{dag_filter}
        ),
        bucket_counts AS (
            SELECT
                CAST(strftime(minute_bucket, '%H') AS INTEGER) * 60
                    + CAST(strftime(minute_bucket, '%M') AS INTEGER) AS minute_of_day,
                CAST(date(minute_bucket) AS VARCHAR) AS bucket_date,
                COUNT(*) AS running_task_count
            FROM task_buckets
            GROUP BY minute_of_day, bucket_date
        )
        SELECT
            minute_of_day,
            MAX(running_task_count) AS peak_running_task_count
        FROM bucket_counts
        GROUP BY minute_of_day
        ORDER BY minute_of_day
        """
    ).fetchall()
    return {int(row[0]): float(row[1]) for row in rows}


def load_observed_per_dag_task_peak_profiles(
    connection: duckdb.DuckDBPyConnection,
    bucket_minutes: int,
    dag_ids: set[str],
) -> dict[str, GlobalPressureProfile]:
    if not dag_ids:
        return {}

    dag_id_list = ", ".join(sql_literal(dag_id) for dag_id in sorted(dag_ids))
    rows = connection.execute(
        f"""
        -- OBSERVED_PER_DAG_TASK_PEAK_PROFILE
        WITH task_buckets AS (
            SELECT
                ti.dag_id,
                gs.minute_bucket
            FROM task_instances_enriched ti,
            LATERAL generate_series(
                date_trunc('minute', ti.start_date AT TIME ZONE 'UTC'),
                date_trunc('minute', (ti.end_date AT TIME ZONE 'UTC') - INTERVAL '1 second'),
                INTERVAL '{bucket_minutes} minute'
            ) AS gs(minute_bucket)
            WHERE ti.start_date IS NOT NULL
              AND ti.end_date IS NOT NULL
              AND ti.end_date > ti.start_date
              AND ti.dag_id IN ({dag_id_list})
        ),
        bucket_counts AS (
            SELECT
                dag_id,
                CAST(strftime(minute_bucket, '%H') AS INTEGER) * 60
                    + CAST(strftime(minute_bucket, '%M') AS INTEGER) AS minute_of_day,
                CAST(date(minute_bucket) AS VARCHAR) AS bucket_date,
                COUNT(*) AS running_task_count
            FROM task_buckets
            GROUP BY dag_id, minute_of_day, bucket_date
        )
        SELECT
            dag_id,
            minute_of_day,
            MAX(running_task_count) AS peak_running_task_count
        FROM bucket_counts
        GROUP BY dag_id, minute_of_day
        ORDER BY dag_id, minute_of_day
        """
    ).fetchall()

    profiles: dict[str, GlobalPressureProfile] = defaultdict(dict)
    for dag_id, minute_of_day, peak_running_task_count in rows:
        profiles[str(dag_id)][int(minute_of_day)] = float(peak_running_task_count)
    return dict(profiles)


def load_observed_effective_start_profile(
    connection: duckdb.DuckDBPyConnection,
    bucket_minutes: int,
    dag_ids: set[str] | None = None,
    exclude_dag_ids: set[str] | None = None,
) -> GlobalPressureProfile:
    if dag_ids is not None and exclude_dag_ids is not None:
        raise ValueError("dag_ids and exclude_dag_ids cannot both be set")

    dag_filter = ""
    query_tag = "OBSERVED_GLOBAL_EFFECTIVE_START_PROFILE"
    if dag_ids is not None:
        if not dag_ids:
            return {}
        dag_id_list = ", ".join(sql_literal(dag_id) for dag_id in sorted(dag_ids))
        dag_filter = f"\n          AND effective_starts.dag_id IN ({dag_id_list})"
        query_tag = "OBSERVED_SCOPED_EFFECTIVE_START_PROFILE"
    elif exclude_dag_ids is not None:
        dag_id_list = ", ".join(sql_literal(dag_id) for dag_id in sorted(exclude_dag_ids))
        dag_filter = f"\n          AND effective_starts.dag_id NOT IN ({dag_id_list})"
        query_tag = "OBSERVED_NON_SCOPED_EFFECTIVE_START_PROFILE"

    rows = connection.execute(
        f"""
        -- {query_tag}
        WITH create_config_starts AS (
            SELECT
                ti.dag_id,
                ti.run_id,
                MIN(ti.start_date) AS effective_start_time
            FROM task_instances_enriched ti
            WHERE ti.start_date IS NOT NULL
              AND ti.task_id IN ('create_config', 'create_run_config')
            GROUP BY ti.dag_id, ti.run_id
        ),
        effective_starts AS (
            SELECT
                dr.dag_id,
                dr.run_id,
                COALESCE(ccs.effective_start_time, dr.start_date) AS effective_start_time
            FROM dag_runs_enriched dr
            LEFT JOIN create_config_starts ccs
              ON ccs.dag_id = dr.dag_id
             AND ccs.run_id = dr.run_id
            WHERE COALESCE(ccs.effective_start_time, dr.start_date) IS NOT NULL
              AND dr.end_date IS NOT NULL
              AND dr.end_date > COALESCE(ccs.effective_start_time, dr.start_date)
        ),
        bucket_counts AS (
            SELECT
                CAST(strftime(date_trunc('minute', effective_start_time AT TIME ZONE 'UTC'), '%H') AS INTEGER) * 60
                    + CAST(strftime(date_trunc('minute', effective_start_time AT TIME ZONE 'UTC'), '%M') AS INTEGER) AS minute_of_day,
                CAST(date(effective_start_time AT TIME ZONE 'UTC') AS VARCHAR) AS bucket_date,
                COUNT(*) AS effective_start_count
            FROM effective_starts
            WHERE TRUE{dag_filter}
            GROUP BY minute_of_day, bucket_date
        )
        SELECT
            CAST(FLOOR(minute_of_day / {bucket_minutes}) * {bucket_minutes} AS INTEGER) AS minute_of_day,
            MAX(effective_start_count) AS peak_effective_start_count
        FROM bucket_counts
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchall()

    return {int(row[0]): float(row[1]) for row in rows}


def load_recent_observed_effective_start_minutes(
    connection: duckdb.DuckDBPyConnection,
    dag_ids: set[str],
    sample_limit: int = 5,
) -> dict[str, int]:
    if not dag_ids:
        return {}

    dag_id_list = ", ".join(sql_literal(dag_id) for dag_id in sorted(dag_ids))
    rows = connection.execute(
        f"""
        -- RECENT_OBSERVED_EFFECTIVE_START_MINUTES
        WITH create_config_starts AS (
            SELECT
                ti.dag_id,
                ti.run_id,
                MIN(ti.start_date) AS effective_start_time
            FROM task_instances_enriched ti
            WHERE ti.start_date IS NOT NULL
              AND ti.task_id IN ('create_config', 'create_run_config')
            GROUP BY ti.dag_id, ti.run_id
        ),
        effective_starts AS (
            SELECT
                dr.dag_id,
                dr.run_id,
                COALESCE(ccs.effective_start_time, dr.start_date) AS effective_start_time,
                ROW_NUMBER() OVER (
                    PARTITION BY dr.dag_id
                    ORDER BY COALESCE(dr.logical_date, dr.start_date) DESC, dr.run_id DESC
                ) AS recency_rank
            FROM dag_runs_enriched dr
            LEFT JOIN create_config_starts ccs
              ON ccs.dag_id = dr.dag_id
             AND ccs.run_id = dr.run_id
            WHERE dr.dag_id IN ({dag_id_list})
              AND COALESCE(ccs.effective_start_time, dr.start_date) IS NOT NULL
              AND dr.end_date IS NOT NULL
              AND dr.end_date > COALESCE(ccs.effective_start_time, dr.start_date)
        ),
        recent_effective_starts AS (
            SELECT
                dag_id,
                CAST(strftime(date_trunc('minute', effective_start_time AT TIME ZONE 'UTC'), '%H') AS INTEGER) * 60
                    + CAST(strftime(date_trunc('minute', effective_start_time AT TIME ZONE 'UTC'), '%M') AS INTEGER) AS minute_of_day
            FROM effective_starts
            WHERE recency_rank <= {int(sample_limit)}
        )
        SELECT
            dag_id,
            CAST(ROUND(QUANTILE_CONT(minute_of_day, 0.25)) AS INTEGER) AS recent_effective_start_minute
        FROM recent_effective_starts
        GROUP BY dag_id
        ORDER BY dag_id
        """
    ).fetchall()

    return {str(dag_id): int(recent_effective_start_minute) for dag_id, recent_effective_start_minute in rows}


def load_representative_task_intervals(
    connection: duckdb.DuckDBPyConnection,
    representative_profiles: dict[str, RepresentativeRunProfile | None],
) -> dict[tuple[str, str], list[tuple[datetime, datetime]]]:
    profiles = [profile for profile in representative_profiles.values() if profile is not None and profile.run_id]
    return load_task_intervals_for_profiles(connection, profiles)


def load_task_intervals_for_profiles(
    connection: duckdb.DuckDBPyConnection,
    profiles: list[RepresentativeRunProfile],
) -> dict[tuple[str, str], list[tuple[datetime, datetime]]]:
    profile_pairs = sorted({(profile.dag_id, profile.run_id) for profile in profiles if profile.run_id})
    if not profile_pairs:
        return {}

    pair_filter = " OR ".join(
        f"(ti.dag_id = {sql_literal(dag_id)} AND ti.run_id = {sql_literal(run_id)})"
        for dag_id, run_id in profile_pairs
    )
    rows = connection.execute(
        f"""
        -- REPRESENTATIVE_TASK_INTERVALS
        SELECT
            ti.dag_id,
            ti.run_id,
            CAST(ti.start_date AS VARCHAR) AS start_date,
            CAST(ti.end_date AS VARCHAR) AS end_date
        FROM task_instances_enriched ti
        WHERE ti.start_date IS NOT NULL
          AND ti.end_date IS NOT NULL
          AND ti.end_date > ti.start_date
          AND ({pair_filter})
        ORDER BY ti.dag_id, ti.run_id, ti.start_date
        """
    ).fetchall()
    intervals: dict[tuple[str, str], list[tuple[datetime, datetime]]] = defaultdict(list)
    for dag_id, run_id, start_date, end_date in rows:
        start_dt = coerce_datetime(start_date)
        end_dt = coerce_datetime(end_date)
        if start_dt is None or end_dt is None or end_dt <= start_dt:
            continue
        intervals[(str(dag_id), str(run_id))].append((start_dt, end_dt))
    return dict(intervals)


def load_task_intervals_by_dag(
    connection: duckdb.DuckDBPyConnection,
    dag_ids: set[str] | None = None,
) -> dict[str, list[tuple[datetime, datetime]]]:
    dag_filter = ""
    if dag_ids is not None:
        if not dag_ids:
            return {}
        dag_id_list = ", ".join(sql_literal(dag_id) for dag_id in sorted(dag_ids))
        dag_filter = f"\n          AND ti.dag_id IN ({dag_id_list})"

    rows = connection.execute(
        f"""
        -- TASK_INTERVALS_BY_DAG
        SELECT
            ti.dag_id,
            CAST(ti.start_date AS VARCHAR) AS start_date,
            CAST(ti.end_date AS VARCHAR) AS end_date
        FROM task_instances_enriched ti
        WHERE ti.start_date IS NOT NULL
          AND ti.end_date IS NOT NULL
          AND ti.end_date > ti.start_date{dag_filter}
        ORDER BY ti.dag_id, ti.start_date
        """
    ).fetchall()

    intervals: dict[str, list[tuple[datetime, datetime]]] = defaultdict(list)
    for dag_id, start_date, end_date in rows:
        start_dt = coerce_datetime(start_date)
        end_dt = coerce_datetime(end_date)
        if start_dt is None or end_dt is None or end_dt <= start_dt:
            continue
        intervals[str(dag_id)].append((start_dt, end_dt))
    return dict(intervals)


def load_observed_global_task_peak(connection: duckdb.DuckDBPyConnection) -> ObservedPeak:
    rows = connection.execute(
        """
        -- OBSERVED_GLOBAL_TASK_PEAK
        WITH task_events AS (
            SELECT ti.start_date AS event_time, 1 AS delta
            FROM task_instances_enriched ti
            WHERE ti.start_date IS NOT NULL
              AND ti.end_date IS NOT NULL
              AND ti.end_date > ti.start_date

            UNION ALL

            SELECT ti.end_date AS event_time, -1 AS delta
            FROM task_instances_enriched ti
            WHERE ti.start_date IS NOT NULL
              AND ti.end_date IS NOT NULL
              AND ti.end_date > ti.start_date
        ),
        event_totals AS (
            SELECT event_time, SUM(delta) AS delta
            FROM task_events
            GROUP BY event_time
        ),
        running_counts AS (
            SELECT
                event_time,
                SUM(delta) OVER (
                    ORDER BY event_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS active_count
            FROM event_totals
        ),
        ranked AS (
            SELECT
                active_count,
                CAST(event_time AS VARCHAR) AS peak_time,
                ROW_NUMBER() OVER (ORDER BY active_count DESC, event_time) AS peak_rank
            FROM running_counts
        )
        SELECT active_count, peak_time
        FROM ranked
        WHERE peak_rank = 1
        """
    ).fetchall()
    if not rows:
        return ObservedPeak(subject="all_dags", observed_peak=0, peak_time="")
    return ObservedPeak(subject="all_dags", observed_peak=int(rows[0][0]), peak_time=str(rows[0][1]))


def load_observed_scoped_task_peak(
    connection: duckdb.DuckDBPyConnection,
    dag_ids: set[str],
    scope_id: str,
) -> ObservedPeak:
    if not dag_ids:
        return ObservedPeak(subject=scope_id, observed_peak=0, peak_time="")

    dag_id_list = ", ".join(sql_literal(dag_id) for dag_id in sorted(dag_ids))
    rows = connection.execute(
        f"""
        -- OBSERVED_SCOPED_TASK_PEAK
        WITH task_events AS (
            SELECT ti.start_date AS event_time, 1 AS delta
            FROM task_instances_enriched ti
            WHERE ti.start_date IS NOT NULL
              AND ti.end_date IS NOT NULL
              AND ti.end_date > ti.start_date
              AND ti.dag_id IN ({dag_id_list})

            UNION ALL

            SELECT ti.end_date AS event_time, -1 AS delta
            FROM task_instances_enriched ti
            WHERE ti.start_date IS NOT NULL
              AND ti.end_date IS NOT NULL
              AND ti.end_date > ti.start_date
              AND ti.dag_id IN ({dag_id_list})
        ),
        event_totals AS (
            SELECT event_time, SUM(delta) AS delta
            FROM task_events
            GROUP BY event_time
        ),
        running_counts AS (
            SELECT
                event_time,
                SUM(delta) OVER (
                    ORDER BY event_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS active_count
            FROM event_totals
        ),
        ranked AS (
            SELECT
                active_count,
                CAST(event_time AS VARCHAR) AS peak_time,
                ROW_NUMBER() OVER (ORDER BY active_count DESC, event_time) AS peak_rank
            FROM running_counts
        )
        SELECT active_count, peak_time
        FROM ranked
        WHERE peak_rank = 1
        """
    ).fetchall()
    if not rows:
        return ObservedPeak(subject=scope_id, observed_peak=0, peak_time="")
    return ObservedPeak(subject=scope_id, observed_peak=int(rows[0][0]), peak_time=str(rows[0][1]))


def load_observed_per_dag_task_peaks(
    connection: duckdb.DuckDBPyConnection,
    dag_ids: set[str],
) -> dict[str, ObservedPeak]:
    if not dag_ids:
        return {}

    dag_id_list = ", ".join(sql_literal(dag_id) for dag_id in sorted(dag_ids))
    rows = connection.execute(
        f"""
        -- OBSERVED_PER_DAG_TASK_PEAK
        WITH task_events AS (
            SELECT ti.dag_id, ti.start_date AS event_time, 1 AS delta
            FROM task_instances_enriched ti
            WHERE ti.start_date IS NOT NULL
              AND ti.end_date IS NOT NULL
              AND ti.end_date > ti.start_date
              AND ti.dag_id IN ({dag_id_list})

            UNION ALL

            SELECT ti.dag_id, ti.end_date AS event_time, -1 AS delta
            FROM task_instances_enriched ti
            WHERE ti.start_date IS NOT NULL
              AND ti.end_date IS NOT NULL
              AND ti.end_date > ti.start_date
              AND ti.dag_id IN ({dag_id_list})
        ),
        event_totals AS (
            SELECT dag_id, event_time, SUM(delta) AS delta
            FROM task_events
            GROUP BY dag_id, event_time
        ),
        running_counts AS (
            SELECT
                dag_id,
                event_time,
                SUM(delta) OVER (
                    PARTITION BY dag_id
                    ORDER BY event_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS active_count
            FROM event_totals
        ),
        ranked AS (
            SELECT
                dag_id,
                active_count,
                CAST(event_time AS VARCHAR) AS peak_time,
                ROW_NUMBER() OVER (
                    PARTITION BY dag_id
                    ORDER BY active_count DESC, event_time
                ) AS peak_rank
            FROM running_counts
        )
        SELECT dag_id, active_count, peak_time
        FROM ranked
        WHERE peak_rank = 1
        ORDER BY dag_id
        """
    ).fetchall()
    return {
        str(row[0]): ObservedPeak(subject=str(row[0]), observed_peak=int(row[1]), peak_time=str(row[2]))
        for row in rows
    }


def load_observed_per_dag_run_peaks(
    connection: duckdb.DuckDBPyConnection,
    dag_ids: set[str],
) -> dict[str, ObservedPeak]:
    if not dag_ids:
        return {}

    dag_id_list = ", ".join(sql_literal(dag_id) for dag_id in sorted(dag_ids))
    rows = connection.execute(
        f"""
        -- OBSERVED_PER_DAG_RUN_PEAK
        WITH run_events AS (
            SELECT dr.dag_id, dr.start_date AS event_time, 1 AS delta
            FROM dag_runs_enriched dr
            WHERE dr.start_date IS NOT NULL
              AND dr.end_date IS NOT NULL
              AND dr.end_date > dr.start_date
              AND dr.dag_id IN ({dag_id_list})

            UNION ALL

            SELECT dr.dag_id, dr.end_date AS event_time, -1 AS delta
            FROM dag_runs_enriched dr
            WHERE dr.start_date IS NOT NULL
              AND dr.end_date IS NOT NULL
              AND dr.end_date > dr.start_date
              AND dr.dag_id IN ({dag_id_list})
        ),
        event_totals AS (
            SELECT dag_id, event_time, SUM(delta) AS delta
            FROM run_events
            GROUP BY dag_id, event_time
        ),
        running_counts AS (
            SELECT
                dag_id,
                event_time,
                SUM(delta) OVER (
                    PARTITION BY dag_id
                    ORDER BY event_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS active_count
            FROM event_totals
        ),
        ranked AS (
            SELECT
                dag_id,
                active_count,
                CAST(event_time AS VARCHAR) AS peak_time,
                ROW_NUMBER() OVER (
                    PARTITION BY dag_id
                    ORDER BY active_count DESC, event_time
                ) AS peak_rank
            FROM running_counts
        )
        SELECT dag_id, active_count, peak_time
        FROM ranked
        WHERE peak_rank = 1
        ORDER BY dag_id
        """
    ).fetchall()
    return {
        str(row[0]): ObservedPeak(subject=str(row[0]), observed_peak=int(row[1]), peak_time=str(row[2]))
        for row in rows
    }
