-- The units a pipeline was decided to run when the case scheduler run `run_id` discovered its cases: what a child
-- triggered by that run may launch (RAD-288). `unit_id` is the calling task for somatic_snv, NULL otherwise.
SELECT DISTINCT unit_id, case_id
FROM {{ mapping.starrocks_pipeline_decision_log }}
WHERE run_id = %(run_id)s
  AND stage = 'discover'
  AND pipeline = %(pipeline)s
  AND scope = 'case'
  AND decision = 'run'
