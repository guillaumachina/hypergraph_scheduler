CREATE OR REPLACE VIEW recommendation_engine_graph_nodes AS
SELECT
    n.dag_id,
    n.repo,
    n.schedule_resolved,
    n.category,
    COALESCE(o.fixed_schedule, TRUE) AS fixed_schedule,
    CASE
        WHEN n.category = 'seed' AND COALESCE(o.fixed_schedule, FALSE) = FALSE THEN TRUE
        ELSE FALSE
    END AS is_reschedulable
FROM raw_recommendation_engine_graph_nodes n
LEFT JOIN raw_recommendation_engine_optimization_dags o
  ON o.dag_id = n.dag_id;

CREATE OR REPLACE VIEW recommendation_engine_reschedulable_dags AS
SELECT *
FROM recommendation_engine_graph_nodes
WHERE is_reschedulable;

CREATE OR REPLACE VIEW recommendation_engine_upstream_context_dags AS
SELECT *
FROM recommendation_engine_graph_nodes
WHERE NOT is_reschedulable;

CREATE OR REPLACE VIEW recommendation_engine_dependency_edges AS
SELECT
    e.from_dag_id,
    upstream.repo AS from_repo,
    upstream.category AS from_category,
    e.to_dag_id,
    downstream.repo AS to_repo,
    downstream.category AS to_category,
    downstream.is_reschedulable AS to_is_reschedulable,
    e.external_task_id,
    e.alignment_type,
    e.alignment_value,
    e.alignment_detail,
    e.depth_from_seed,
    e.enforced_envs
FROM raw_recommendation_engine_graph_edges e
LEFT JOIN recommendation_engine_graph_nodes upstream
  ON upstream.dag_id = e.from_dag_id
LEFT JOIN recommendation_engine_graph_nodes downstream
  ON downstream.dag_id = e.to_dag_id;

CREATE OR REPLACE VIEW recommendation_engine_runtime_summary AS
SELECT
    n.dag_id,
    n.repo,
    n.category,
    n.schedule_resolved,
    n.fixed_schedule,
    n.is_reschedulable,
    r.scheduled_run_count,
    r.success_run_count,
    r.non_success_run_count,
    r.avg_queue_delay_seconds,
    r.avg_start_delay_seconds,
    r.avg_dag_runtime_seconds,
    r.median_dag_runtime_seconds,
    r.p90_dag_runtime_seconds,
    r.avg_schedule_to_end_seconds,
    r.median_schedule_to_end_seconds,
    r.p90_schedule_to_end_seconds,
    r.first_seen_start_date,
    r.last_seen_end_date
FROM recommendation_engine_graph_nodes n
LEFT JOIN dag_runtime_summary r
  ON r.dag_id = n.dag_id;

CREATE OR REPLACE VIEW recommendation_engine_reschedulable_runtime_summary AS
SELECT *
FROM recommendation_engine_runtime_summary
WHERE is_reschedulable;

CREATE OR REPLACE VIEW recommendation_engine_sensor_bottleneck_summary AS
SELECT
    s.dag_id,
    n.repo,
    n.category,
    n.is_reschedulable,
    s.task_id,
    s.operator_name,
    s.sensor_run_count,
    s.success_sensor_run_count,
    s.avg_reschedule_count,
    s.avg_active_poke_seconds,
    s.avg_sensor_elapsed_seconds,
    s.median_sensor_elapsed_seconds,
    s.avg_idle_wait_seconds,
    s.median_idle_wait_seconds,
    s.p90_idle_wait_seconds,
    s.max_idle_wait_seconds,
    s.total_idle_wait_seconds,
    s.last_seen_reschedule_end
FROM sensor_bottleneck_summary s
JOIN recommendation_engine_graph_nodes n
  ON n.dag_id = s.dag_id;

CREATE OR REPLACE VIEW recommendation_engine_effective_start_summary AS
SELECT
    dag_id,
    COUNT(*) AS create_config_run_count,
    AVG(schedule_to_task_start_seconds) AS avg_effective_start_delay_seconds,
    MEDIAN(schedule_to_task_start_seconds) AS median_effective_start_delay_seconds,
    QUANTILE_CONT(schedule_to_task_start_seconds, 0.9) AS p90_effective_start_delay_seconds,
    MIN(schedule_to_task_start_seconds) AS min_effective_start_delay_seconds,
    MAX(schedule_to_task_start_seconds) AS max_effective_start_delay_seconds
FROM task_instances_enriched
WHERE task_id = 'create_config'
  AND dag_id IN (SELECT dag_id FROM recommendation_engine_graph_nodes)
GROUP BY dag_id;

CREATE OR REPLACE VIEW recommendation_engine_effective_processing_summary AS
WITH create_config AS (
    SELECT
        dag_id,
        run_id,
        start_date AS create_config_start
    FROM task_instances_enriched
    WHERE task_id = 'create_config'
      AND dag_id IN (SELECT dag_id FROM recommendation_engine_graph_nodes)
)
SELECT
    dr.dag_id,
    COUNT(*) AS successful_run_count,
    AVG(EXTRACT(EPOCH FROM (dr.end_date - cc.create_config_start))) AS avg_effective_processing_seconds,
    MEDIAN(EXTRACT(EPOCH FROM (dr.end_date - cc.create_config_start))) AS median_effective_processing_seconds,
    QUANTILE_CONT(EXTRACT(EPOCH FROM (dr.end_date - cc.create_config_start)), 0.9) AS p90_effective_processing_seconds
FROM dag_runs_enriched dr
JOIN create_config cc
  ON cc.dag_id = dr.dag_id
 AND cc.run_id = dr.run_id
WHERE dr.state = 'success'
  AND dr.end_date IS NOT NULL
  AND cc.create_config_start IS NOT NULL
GROUP BY dr.dag_id;

CREATE OR REPLACE VIEW recommendation_engine_rescheduling_candidates AS
SELECT
    r.dag_id,
    r.repo,
    r.schedule_resolved,
    r.scheduled_run_count,
    r.avg_dag_runtime_seconds,
    r.p90_dag_runtime_seconds,
    r.avg_schedule_to_end_seconds,
    r.p90_schedule_to_end_seconds,
    COUNT(DISTINCT e.from_dag_id) AS direct_upstream_dependency_count,
    COUNT(DISTINCT s.task_id) AS scoped_sensor_count,
    es.create_config_run_count,
    es.avg_effective_start_delay_seconds,
    es.median_effective_start_delay_seconds,
    es.p90_effective_start_delay_seconds,
    ep.successful_run_count AS effective_processing_successful_run_count,
    ep.avg_effective_processing_seconds,
    ep.median_effective_processing_seconds,
    ep.p90_effective_processing_seconds,
    COALESCE(SUM(s.total_idle_wait_seconds), 0) AS total_scoped_idle_wait_seconds,
    COALESCE(MAX(s.p90_idle_wait_seconds), 0) AS max_sensor_p90_idle_wait_seconds
FROM recommendation_engine_reschedulable_runtime_summary r
LEFT JOIN recommendation_engine_dependency_edges e
  ON e.to_dag_id = r.dag_id
LEFT JOIN recommendation_engine_sensor_bottleneck_summary s
  ON s.dag_id = r.dag_id
LEFT JOIN recommendation_engine_effective_start_summary es
  ON es.dag_id = r.dag_id
LEFT JOIN recommendation_engine_effective_processing_summary ep
  ON ep.dag_id = r.dag_id
GROUP BY
    r.dag_id,
    r.repo,
    r.schedule_resolved,
    r.scheduled_run_count,
    r.avg_dag_runtime_seconds,
    r.p90_dag_runtime_seconds,
    r.avg_schedule_to_end_seconds,
    r.p90_schedule_to_end_seconds,
    es.create_config_run_count,
    es.avg_effective_start_delay_seconds,
    es.median_effective_start_delay_seconds,
    es.p90_effective_start_delay_seconds,
    ep.successful_run_count,
    ep.avg_effective_processing_seconds,
    ep.median_effective_processing_seconds,
    ep.p90_effective_processing_seconds;

CREATE OR REPLACE VIEW recommendation_engine_seed_edge_sensor_map AS
SELECT *
FROM (
    VALUES
        ('pipeline_end', 'market_item_recommender', 'wait_for_dwh_loaded'),
        ('custom_reports', 'recipe_recommender', 'wait_pipeline_end.wait_for_custom_reports'),
        ('customer_feature_groups_sf', 'recipe_recommender', 'wait_pipeline_end.wait_for_customer_fg_sf'),
        ('recipe_feature_groups_sf', 'recipe_recommender', 'wait_pipeline_end.wait_for_recipe_fg_sf'),
        ('customer_feature_groups_sf', 'relevance_scoring', 'wait_pipeline_end.wait_for_customer_fg_sf'),
        ('recipe_feature_groups_sf', 'relevance_scoring', 'wait_pipeline_end.wait_for_recipe_fg_sf'),
        ('customer_feature_groups_sf', 'user_clustering_predict', 'wait_for_customer_fg_sf')
) AS t(from_dag_id, to_dag_id, sensor_task_id);

CREATE OR REPLACE VIEW recommendation_engine_seed_edge_waits AS
WITH sensor_waits AS (
    SELECT
        dag_id,
        task_id,
        COUNT(*) AS sensor_run_count,
    AVG(schedule_to_sensor_start_seconds) AS avg_schedule_to_sensor_start_seconds,
    MEDIAN(schedule_to_sensor_start_seconds) AS median_schedule_to_sensor_start_seconds,
    QUANTILE_CONT(schedule_to_sensor_start_seconds, 0.9) AS p90_schedule_to_sensor_start_seconds,
        AVG(idle_wait_seconds) AS avg_idle_wait_seconds,
        MEDIAN(idle_wait_seconds) AS median_idle_wait_seconds,
        QUANTILE_CONT(idle_wait_seconds, 0.9) AS p90_idle_wait_seconds,
        MAX(idle_wait_seconds) AS max_idle_wait_seconds,
        SUM(idle_wait_seconds) AS total_idle_wait_seconds
    FROM sensor_wait_summary
    GROUP BY dag_id, task_id
)
SELECT
    e.from_dag_id,
    e.to_dag_id,
    downstream.repo AS to_repo,
    downstream.schedule_resolved AS downstream_schedule,
    m.sensor_task_id,
    s.sensor_run_count,
    s.avg_schedule_to_sensor_start_seconds,
    s.median_schedule_to_sensor_start_seconds,
    s.p90_schedule_to_sensor_start_seconds,
    s.avg_idle_wait_seconds,
    s.median_idle_wait_seconds,
    s.p90_idle_wait_seconds,
    s.max_idle_wait_seconds,
    s.total_idle_wait_seconds
FROM recommendation_engine_dependency_edges e
JOIN recommendation_engine_reschedulable_dags downstream
  ON downstream.dag_id = e.to_dag_id
LEFT JOIN recommendation_engine_seed_edge_sensor_map m
  ON m.from_dag_id = e.from_dag_id
 AND m.to_dag_id = e.to_dag_id
LEFT JOIN sensor_waits s
  ON s.dag_id = m.to_dag_id
 AND s.task_id = m.sensor_task_id;

CREATE OR REPLACE VIEW recommendation_engine_candidate_report AS
SELECT
    c.dag_id,
    c.repo,
    c.schedule_resolved,
    c.scheduled_run_count,
    c.avg_dag_runtime_seconds,
    c.p90_dag_runtime_seconds,
    c.avg_schedule_to_end_seconds,
    c.p90_schedule_to_end_seconds,
    c.create_config_run_count,
    c.avg_effective_start_delay_seconds,
    c.median_effective_start_delay_seconds,
    c.p90_effective_start_delay_seconds,
    c.effective_processing_successful_run_count,
    c.avg_effective_processing_seconds,
    c.median_effective_processing_seconds,
    c.p90_effective_processing_seconds,
    c.direct_upstream_dependency_count,
    c.scoped_sensor_count,
    c.total_scoped_idle_wait_seconds,
    c.max_sensor_p90_idle_wait_seconds,
    COALESCE(SUM(w.total_idle_wait_seconds), 0) AS mapped_upstream_idle_wait_seconds,
    COALESCE(MAX(w.p90_idle_wait_seconds), 0) AS mapped_edge_max_p90_idle_wait_seconds,
    COALESCE(MAX(w.avg_schedule_to_sensor_start_seconds + w.avg_idle_wait_seconds), 0) AS mapped_edge_max_avg_ready_seconds,
    COALESCE(MAX(w.p90_schedule_to_sensor_start_seconds + w.p90_idle_wait_seconds), 0) AS mapped_edge_max_p90_ready_seconds,
    COALESCE(MAX(w.avg_schedule_to_sensor_start_seconds), 0) AS mapped_edge_max_avg_sensor_touch_seconds,
    COALESCE(MAX(w.p90_schedule_to_sensor_start_seconds), 0) AS mapped_edge_max_p90_sensor_touch_seconds
FROM recommendation_engine_rescheduling_candidates c
LEFT JOIN recommendation_engine_seed_edge_waits w
  ON w.to_dag_id = c.dag_id
GROUP BY
    c.dag_id,
    c.repo,
    c.schedule_resolved,
    c.scheduled_run_count,
    c.avg_dag_runtime_seconds,
    c.p90_dag_runtime_seconds,
    c.avg_schedule_to_end_seconds,
    c.p90_schedule_to_end_seconds,
  c.create_config_run_count,
  c.avg_effective_start_delay_seconds,
  c.median_effective_start_delay_seconds,
  c.p90_effective_start_delay_seconds,
    c.effective_processing_successful_run_count,
    c.avg_effective_processing_seconds,
    c.median_effective_processing_seconds,
    c.p90_effective_processing_seconds,
    c.direct_upstream_dependency_count,
    c.scoped_sensor_count,
    c.total_scoped_idle_wait_seconds,
    c.max_sensor_p90_idle_wait_seconds;

CREATE OR REPLACE VIEW recommendation_engine_optimization_inputs AS
SELECT
    c.dag_id,
    c.repo,
    c.schedule_resolved,
    g.fixed_schedule,
    g.is_reschedulable,
    c.scheduled_run_count,
    c.avg_dag_runtime_seconds,
    c.p90_dag_runtime_seconds,
    c.avg_schedule_to_end_seconds,
    c.p90_schedule_to_end_seconds,
  c.create_config_run_count,
  c.avg_effective_start_delay_seconds,
  c.median_effective_start_delay_seconds,
  c.p90_effective_start_delay_seconds,
    c.effective_processing_successful_run_count,
    c.avg_effective_processing_seconds,
    c.median_effective_processing_seconds,
    c.p90_effective_processing_seconds,
    c.direct_upstream_dependency_count,
    c.scoped_sensor_count,
    c.total_scoped_idle_wait_seconds,
    c.max_sensor_p90_idle_wait_seconds,
    c.mapped_upstream_idle_wait_seconds,
    c.mapped_edge_max_p90_idle_wait_seconds,
    c.mapped_edge_max_avg_ready_seconds,
    c.mapped_edge_max_p90_ready_seconds,
    c.mapped_edge_max_avg_sensor_touch_seconds,
    c.mapped_edge_max_p90_sensor_touch_seconds
FROM recommendation_engine_candidate_report c
JOIN recommendation_engine_graph_nodes g
  ON g.dag_id = c.dag_id;
