from __future__ import annotations

import duckdb

from hypergraph_scheduler.duckdb_pipeline import load_scope_static_inputs
from hypergraph_scheduler.scopes import get_scope


def test_load_scope_static_inputs_populates_duckdb_tables() -> None:
    connection = duckdb.connect(":memory:")
    scope = get_scope("recommendation_engine")

    load_scope_static_inputs(connection, scope)

    node_count = connection.execute("SELECT COUNT(*) FROM raw_recommendation_engine_graph_nodes").fetchone()[0]
    edge_count = connection.execute("SELECT COUNT(*) FROM raw_recommendation_engine_graph_edges").fetchone()[0]
    dag_count = connection.execute("SELECT COUNT(*) FROM raw_recommendation_engine_optimization_dags").fetchone()[0]
    sensor_map_count = connection.execute("SELECT COUNT(*) FROM raw_recommendation_engine_seed_edge_sensor_map").fetchone()[0]
    recipe_row = connection.execute(
        """
        SELECT dag_id, scheduled_cron, fixed_schedule
        FROM raw_recommendation_engine_optimization_dags
        WHERE dag_id = 'recipe_recommender'
        """
    ).fetchone()

    assert node_count > 0
    assert edge_count > 0
    assert dag_count > 0
    assert sensor_map_count > 0
    assert recipe_row is not None
    assert recipe_row[0] == "recipe_recommender"
    assert recipe_row[1]
    assert recipe_row[2] in (True, False, None)

    connection.close()