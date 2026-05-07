# Recommendation Engine Rescheduling Report

This report scopes optimization candidates to DS-owned recommendation_engine DAGs and treats upstream DAGs as fixed context.

## Candidate Summary

| DAG | Schedule | Runs | Avg runtime s | P90 runtime s | Avg effective start h | P90 effective start h | Total sensor idle wait s | Mapped upstream idle wait s | Max mapped edge P90 wait s | Max mapped sensor touch h | Direct upstream deps |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| relevance_scoring | 05 07 * * * | 31 | 10,835.8 | 15,302.9 | 3.47 | 3.96 | 86.2 | 43.1 |
| recipe_recommender | 05 07 * * 3 | 5 | 30,055.2 | 39,863.4 | 3.56 | 4.19 | 131.9 | 35.8 |
| user_clustering_predict | 05 07 * * 3 | 5 | 7,570.3 | 11,543.7 | 2.44 | 3.02 | 3.2 | 3.2 |
| market_item_recommender | 05 04 * * 3 | 6 | 14,268.2 | 20,219.3 | 4.66 | 4.89 | 4.9 | 2.7 |
| menu_ranker | 30 4,18 * * * | 62 | 805.4 | 1,217.0 | 0.04 | 0.00 | 0.0 | 0.0 |

## Edge-Level Wait Pressure

| Upstream DAG | Downstream DAG | Sensor task | Runs | Total idle wait s | P90 idle wait s |
| --- | --- | --- | ---: | ---: | ---: |
| customer_feature_groups_sf | relevance_scoring | wait_pipeline_end.wait_for_customer_fg_sf | 22 | 24.6 | 1.8 |
| recipe_feature_groups_sf | relevance_scoring | wait_pipeline_end.wait_for_recipe_fg_sf | 22 | 18.5 | 1.4 |
| custom_reports | recipe_recommender | wait_pipeline_end.wait_for_custom_reports | 5 | 14.4 | 3.7 |
| customer_feature_groups_sf | recipe_recommender | wait_pipeline_end.wait_for_customer_fg_sf | 5 | 12.2 | 3.3 |
| recipe_feature_groups_sf | recipe_recommender | wait_pipeline_end.wait_for_recipe_fg_sf | 5 | 9.3 | 2.5 |
| customer_feature_groups_sf | user_clustering_predict | wait_for_customer_fg_sf | 4 | 3.2 | 1.3 |
| pipeline_end | market_item_recommender | wait_for_dwh_loaded | 4 | 2.7 | 0.8 |
