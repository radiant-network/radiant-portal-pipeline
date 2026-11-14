SELECT case_id, pipeline, item_key, decision, CAST(reasons AS VARCHAR) AS reasons
FROM {{ mapping.starrocks_pipeline_state }}
WHERE pipeline = %(pipeline)s
