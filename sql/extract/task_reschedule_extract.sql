-- Extract reschedule events for sensor-style tasks from the Airflow metadata DB.
SELECT
    id,
    ti_id,
    start_date,
    end_date,
    duration,
    reschedule_date
FROM task_reschedule;
