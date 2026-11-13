-- The units a pipeline was decided to run by the case scheduler run `run_id` at `stage`: at `discover`, what a child
-- triggered by that run may launch; at `after`, what is still waiting. `unit_id` is the calling task for somatic_snv,
-- NULL otherwise.
SELECT DISTINCT unit_id, case_id
FROM {{ mapping.starrocks_pipeline_decision_log }}
WHERE run_id = %(run_id)s
  AND stage = %(stage)s
  AND pipeline = %(pipeline)s
  AND scope = 'case'
  AND decision = 'run'
