-- One row per (candidate germline case, family member): the facts the case scheduler decides on
-- (`decide_cases`, radiant/tasks/nextflow/cnv/resolve.py). No decision here.
--
-- A candidate is a germline case where a member's current experiment has a germline CNV VCF not annotated for the
-- case. A case where no member has one is not a candidate: CNVs not called, or joint-called upstream. The tenant is
-- returned, not filtered on: an excluded tenant is a reason, decided in Python.
WITH annotated AS (
    -- Scoped to the case, not just the experiment: annotating case 1 must not make case 2 look done.
    SELECT DISTINCT tc.case_id                  AS case_id,
                    tc.sequencing_experiment_id AS seq_id
    FROM {{ mapping.clinical_task }} t
    JOIN {{ mapping.clinical_task_context }} tc ON tc.task_id = t.id
    WHERE t.task_type_code = 'radiant_germline_cnv_annotation'
      AND tc.case_id IS NOT NULL
),
candidate AS (
    SELECT DISTINCT ce.case_id AS case_id
    FROM {{ mapping.starrocks_current_experiment }} ce
    JOIN {{ mapping.starrocks_current_germline_gcnv }} g ON g.seq_id = ce.seq_id
    LEFT JOIN annotated a                               ON a.case_id = ce.case_id
                                                       AND a.seq_id  = ce.seq_id
    WHERE ce.case_type = 'germline'
      AND a.seq_id IS NULL
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
       ca.pipeline_name                AS alignment_pipeline,
       COALESCE(g.matches, 0)          AS gcnv_matches,
       an.seq_id IS NOT NULL           AS annotated,
       ac.case_id IS NOT NULL          AS case_annotated
FROM {{ mapping.starrocks_pipeline_case }} pc
JOIN candidate cd                                        ON cd.case_id = pc.case_id
JOIN {{ mapping.starrocks_case_member }} cm              ON cm.case_id = pc.case_id
-- LEFT: a member with no experiment, alignment or CNV VCF must come back, so its reason is decided.
LEFT JOIN {{ mapping.starrocks_current_experiment }} ce  ON ce.case_id = pc.case_id
                                                        AND ce.patient_id = cm.patient_id
LEFT JOIN {{ mapping.starrocks_current_alignment }} ca   ON ca.seq_id = ce.seq_id
                                                        AND ca.task_type = 'alignment_germline_variant_calling'
LEFT JOIN {{ mapping.starrocks_current_germline_gcnv }} g ON g.seq_id = ce.seq_id
LEFT JOIN annotated an                                   ON an.case_id = pc.case_id
                                                        AND an.seq_id  = ce.seq_id
-- Annotated before, whatever the experiment: tells a case's first run from a later one.
LEFT JOIN (SELECT DISTINCT case_id FROM annotated) ac    ON ac.case_id = pc.case_id
WHERE pc.case_type = 'germline'
ORDER BY pc.case_id, cm.patient_id
