-- Extract scheduled DAG runs and runtime timestamps from the Airflow metadata DB.
SELECT
    id,
    dag_id,
    run_id,
    run_type,
    state,
    logical_date,
    data_interval_start,
    data_interval_end,
    run_after AS scheduled_at,
    queued_at,
    start_date,
    end_date,
    EXTRACT(EPOCH FROM (queued_at - run_after)) AS queue_delay_seconds,
    EXTRACT(EPOCH FROM (start_date - run_after)) AS start_delay_seconds,
    EXTRACT(EPOCH FROM (end_date - start_date)) AS dag_runtime_seconds,
    EXTRACT(EPOCH FROM (end_date - run_after)) AS schedule_to_end_seconds
FROM dag_run
WHERE run_type = 'scheduled';

