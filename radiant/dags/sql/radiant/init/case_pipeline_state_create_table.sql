-- The current state of every unit (a case, or a calling task and case for somatic_snv) for every pipeline: the rows
-- of its last write in `pipeline_decision_log`, its case row and, when blocked, its member rows. The log holds the
-- changes only, so the last write of a unit is its state until the next change.
--
-- The last write is the latest `decided_at`, `after` before `discover` within a run. `unit_id` is NULL but for
-- somatic_snv, hence the null-safe join.
CREATE VIEW IF NOT EXISTS {{ mapping.starrocks_case_pipeline_state }} AS
WITH last_write AS (
    SELECT pipeline, case_id, unit_id, run_id, stage
    FROM (
        SELECT pipeline,
               case_id,
               unit_id,
               run_id,
               stage,
               ROW_NUMBER() OVER (
                   PARTITION BY pipeline, case_id, unit_id
                   ORDER BY decided_at DESC, CASE stage WHEN 'after' THEN 0 ELSE 1 END
               ) AS rn
        FROM {{ mapping.starrocks_pipeline_decision_log }}
        WHERE scope = 'case'
    ) ranked
    WHERE rn = 1
)
SELECT l.run_id,
       l.stage,
       l.pipeline,
       l.case_id,
       l.scope,
       l.tenant_code,
       l.patient_id,
       l.seq_id,
       l.task_id,
       l.unit_id,
       l.decision,
       l.reason,
       l.detail,
       l.decided_at
FROM {{ mapping.starrocks_pipeline_decision_log }} l
JOIN last_write w ON w.pipeline = l.pipeline
                 AND w.case_id = l.case_id
                 AND w.unit_id <=> l.unit_id
                 AND w.run_id = l.run_id
                 AND w.stage = l.stage
