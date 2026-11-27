-- Where each item of the named cases stands: the last journal row of each, for `radiant-case-evaluation`.
SELECT case_id, pipeline, item_key, decision
FROM {{ mapping.starrocks_pipeline_state }}
WHERE case_id IN %(case_ids)s
