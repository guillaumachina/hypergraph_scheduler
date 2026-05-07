-- Derived DuckDB views over raw imported Airflow metadata extracts.

CREATE OR REPLACE VIEW dag_runs_enriched AS
SELECT
    id AS dag_run_id,
    dag_id,
    run_id,
    run_type,
    state,
    logical_date,
    data_interval_start,
    data_interval_end,
    scheduled_at,
    queued_at,
    start_date,
    end_date,
    queue_delay_seconds,
    start_delay_seconds,
    dag_runtime_seconds,
    schedule_to_end_seconds
FROM raw_dag_run;

CREATE OR REPLACE VIEW task_instances_enriched AS
SELECT
    ti.id AS task_instance_id,
    ti.dag_id,
    ti.task_id,
    ti.run_id,
    ti.map_index,
    ti.state,
    ti.try_number,
    ti.max_tries,
    ti.queued_dttm,
    ti.scheduled_dttm,
    ti.start_date,
    ti.end_date,
    ti.duration AS task_duration_seconds,
    COALESCE(ti.custom_operator_name, ti.operator) AS operator_name,
    ti.operator,
    ti.custom_operator_name,
    ti.pool,
    ti.queue,
    ti.priority_weight,
    ti.queued_by_job_id,
    ti.trigger_id,
    ti.trigger_timeout,
    ti.next_method,
    ti.next_kwargs,
    ti.executor,
    ti.updated_at,
    ti.span_status,
    dr.logical_date,
    dr.run_type AS dag_run_type,
    dr.scheduled_at,
    dr.queued_at AS dag_queued_at,
    dr.start_date AS dag_start_date,
    dr.end_date AS dag_end_date,
    dr.dag_run_id,
    dr.dag_run_id IS NOT NULL AS has_dag_run_metadata,
    EXTRACT(EPOCH FROM (ti.start_date - dr.scheduled_at)) AS schedule_to_task_start_seconds,
    EXTRACT(EPOCH FROM (ti.end_date - ti.start_date)) AS task_elapsed_seconds
FROM raw_task_instance ti
LEFT JOIN dag_runs_enriched dr
  ON dr.dag_id = ti.dag_id
 AND dr.run_id = ti.run_id;

CREATE OR REPLACE VIEW task_instance_join_gaps AS
SELECT
    task_instance_id,
    dag_id,
    task_id,
    run_id,
    state,
    operator_name,
    start_date,
    end_date,
    CASE
        WHEN run_id ILIKE 'manual__%' THEN 'manual_run_not_in_scheduled_dag_run_extract'
        ELSE 'missing_dag_run_extract_row'
    END AS join_gap_reason
FROM task_instances_enriched
WHERE NOT has_dag_run_metadata;

CREATE OR REPLACE VIEW task_instance_join_gap_summary AS
SELECT
    join_gap_reason,
    COUNT(*) AS task_instance_count,
    COUNT(DISTINCT dag_id) AS dag_count,
    COUNT(DISTINCT run_id) AS run_count
FROM task_instance_join_gaps
GROUP BY join_gap_reason;

CREATE OR REPLACE VIEW sensor_task_runs AS
SELECT *
FROM task_instances_enriched
WHERE task_id ILIKE 'wait_for_%'
   OR operator_name ILIKE '%Sensor%';

CREATE OR REPLACE VIEW sensor_reschedule_summary AS
SELECT
    ti_id AS task_instance_id,
    COUNT(*) AS reschedule_count,
    SUM(duration) AS active_poke_seconds,
    MIN(start_date) AS first_reschedule_start,
    MAX(end_date) AS last_reschedule_end,
    MAX(reschedule_date) AS last_reschedule_date
FROM raw_task_reschedule
GROUP BY ti_id;

CREATE OR REPLACE VIEW sensor_wait_summary AS
SELECT
    s.task_instance_id,
    s.dag_id,
    s.task_id,
    s.run_id,
    s.logical_date,
    s.state,
    s.try_number,
    s.operator_name,
    s.scheduled_at,
    s.queued_dttm,
    s.start_date,
    s.end_date,
    s.task_duration_seconds,
    COALESCE(r.reschedule_count, 0) AS reschedule_count,
    COALESCE(r.active_poke_seconds, 0) AS active_poke_seconds,
    s.schedule_to_task_start_seconds AS schedule_to_sensor_start_seconds,
    s.task_elapsed_seconds AS sensor_elapsed_seconds,
    GREATEST(s.task_elapsed_seconds - COALESCE(r.active_poke_seconds, 0), 0) AS idle_wait_seconds,
    r.first_reschedule_start,
    r.last_reschedule_end,
    r.last_reschedule_date
FROM sensor_task_runs s
LEFT JOIN sensor_reschedule_summary r
  ON r.task_instance_id = s.task_instance_id;

CREATE OR REPLACE VIEW dag_runtime_summary AS
SELECT
    dag_id,
    COUNT(*) AS scheduled_run_count,
    COUNT(*) FILTER (WHERE state = 'success') AS success_run_count,
    COUNT(*) FILTER (WHERE state != 'success') AS non_success_run_count,
    AVG(queue_delay_seconds) AS avg_queue_delay_seconds,
    AVG(start_delay_seconds) AS avg_start_delay_seconds,
    AVG(dag_runtime_seconds) AS avg_dag_runtime_seconds,
    MEDIAN(dag_runtime_seconds) AS median_dag_runtime_seconds,
    QUANTILE_CONT(dag_runtime_seconds, 0.9) AS p90_dag_runtime_seconds,
    AVG(schedule_to_end_seconds) AS avg_schedule_to_end_seconds,
    MEDIAN(schedule_to_end_seconds) AS median_schedule_to_end_seconds,
    QUANTILE_CONT(schedule_to_end_seconds, 0.9) AS p90_schedule_to_end_seconds,
    MIN(start_date) AS first_seen_start_date,
    MAX(end_date) AS last_seen_end_date
FROM dag_runs_enriched
GROUP BY dag_id;

CREATE OR REPLACE VIEW sensor_bottleneck_summary AS
SELECT
    dag_id,
    task_id,
    operator_name,
    COUNT(*) AS sensor_run_count,
    COUNT(*) FILTER (WHERE state = 'success') AS success_sensor_run_count,
    AVG(reschedule_count) AS avg_reschedule_count,
    AVG(active_poke_seconds) AS avg_active_poke_seconds,
    AVG(sensor_elapsed_seconds) AS avg_sensor_elapsed_seconds,
    MEDIAN(sensor_elapsed_seconds) AS median_sensor_elapsed_seconds,
    AVG(idle_wait_seconds) AS avg_idle_wait_seconds,
    MEDIAN(idle_wait_seconds) AS median_idle_wait_seconds,
    QUANTILE_CONT(idle_wait_seconds, 0.9) AS p90_idle_wait_seconds,
    MAX(idle_wait_seconds) AS max_idle_wait_seconds,
    SUM(idle_wait_seconds) AS total_idle_wait_seconds,
    MAX(last_reschedule_end) AS last_seen_reschedule_end
FROM sensor_wait_summary
GROUP BY dag_id, task_id, operator_name;

