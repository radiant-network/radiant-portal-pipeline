-- One row per (candidate germline case, family member): the facts the case scheduler decides on
-- (`decide_cases`, radiant/tasks/nextflow/resolve.py). No decision here.
--
-- A candidate is a germline case with no experiment yet, or with a member's current experiment not annotated for
-- the case, whether its documents are there or not. The tenant is returned, not filtered on: an excluded tenant is
-- a reason, decided in Python.
WITH annotated AS (
    -- Scoped to the case, not just the experiment: annotating case 1 must not make case 2 look done.
    SELECT DISTINCT tc.case_id                  AS case_id,
                    tc.sequencing_experiment_id AS seq_id
    FROM {{ mapping.clinical_task }} t
    JOIN {{ mapping.clinical_task_context }} tc ON tc.task_id = t.id
    WHERE t.task_type_code = 'radiant_germline_annotation'
      AND tc.case_id IS NOT NULL
),
candidate AS (
    SELECT DISTINCT pc.case_id AS case_id
    FROM {{ mapping.starrocks_pipeline_case }} pc
    LEFT JOIN {{ mapping.starrocks_current_experiment }} ce ON ce.case_id = pc.case_id
    LEFT JOIN annotated a                                ON a.case_id = ce.case_id
                                                        AND a.seq_id  = ce.seq_id
    WHERE pc.case_type = 'germline'
      AND (ce.seq_id IS NULL OR a.seq_id IS NULL)
)
SELECT pc.case_id                      AS case_id,
       pc.submitter_case_id            AS submitter_case_id,
       pc.tenant_code                  AS tenant_code,
       pc.project_code                 AS project_code,
       cm.role                         AS role,
       cm.affected_status              AS affected_status,
       cm.patient_id                   AS patient_id,
       cm.sex                          AS sex,
       ce.seq_id                       AS seq_id,
       ce.aliquot                      AS aliquot,
       ce.strategy                     AS strategy,
       ca.task_id                      AS current_alignment_task_id,
       COALESCE(g.matches, 0)          AS gvcf_matches,
       an.seq_id IS NOT NULL           AS annotated
FROM {{ mapping.starrocks_pipeline_case }} pc
JOIN candidate cd                                        ON cd.case_id = pc.case_id
JOIN {{ mapping.starrocks_case_member }} cm              ON cm.case_id = pc.case_id
-- LEFT: a member with no experiment, alignment or gVCF must come back, so its reason is decided.
LEFT JOIN {{ mapping.starrocks_current_experiment }} ce  ON ce.case_id = pc.case_id
                                                        AND ce.patient_id = cm.patient_id
LEFT JOIN {{ mapping.starrocks_current_alignment }} ca   ON ca.seq_id = ce.seq_id
                                                        AND ca.task_type = 'alignment_germline_variant_calling'
LEFT JOIN {{ mapping.starrocks_current_germline_gvcf }} g ON g.seq_id = ce.seq_id
LEFT JOIN annotated an                                   ON an.case_id = pc.case_id
                                                        AND an.seq_id  = ce.seq_id
WHERE pc.case_type = 'germline'
ORDER BY pc.case_id, cm.patient_id
