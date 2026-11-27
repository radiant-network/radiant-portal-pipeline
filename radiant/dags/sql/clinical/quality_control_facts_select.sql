-- One row per (candidate germline case, family member): the facts the pipeline DAG decides on
-- (`decide_cases`, radiant/tasks/nextflow/qc/resolve.py). No decision here.
--
-- A candidate is a germline case where a member's current experiment has a CRAM on its current alignment and no
-- `quality_control_metrics` task for the case. The tenant is returned, not filtered on: an excluded tenant is a
-- reason, decided in Python.
WITH alignment_document AS (
    -- Every output of the current germline alignment, selected on the task, never on file name.
    SELECT ca.seq_id        AS seq_id,
           d.url            AS url,
           d.data_type_code AS data_type_code,
           d.format_code    AS format_code
    FROM {{ mapping.starrocks_current_alignment }} ca
    JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = ca.task_id
                                                     AND thd.type = 'output'
    JOIN {{ mapping.clinical_document }} d            ON d.id = thd.document_id
    WHERE ca.task_type = 'alignment_germline_variant_calling'
),
document_count AS (
    -- Two CRAMs or two tarballs on one task mean a mistyped document, not a re-alignment.
    SELECT seq_id,
           COUNT(DISTINCT CASE WHEN data_type_code = 'alignment' AND format_code = 'cram' THEN url END) AS cram_matches,
           COUNT(DISTINCT CASE WHEN data_type_code = 'ssup' AND format_code = 'tgz' THEN url END)       AS dragen_metrics_matches
    FROM alignment_document
    GROUP BY seq_id
),
quality_controlled AS (
    -- Scoped to the case, not just the experiment: a shared experiment QC'd under case 1 must not make case 2 look done.
    SELECT DISTINCT tc.case_id                  AS case_id,
                    tc.sequencing_experiment_id AS seq_id
    FROM {{ mapping.clinical_task }} t
    JOIN {{ mapping.clinical_task_context }} tc ON tc.task_id = t.id
    WHERE t.task_type_code = 'quality_control_metrics'
      AND tc.case_id IS NOT NULL
),
candidate AS (
    SELECT DISTINCT ce.case_id AS case_id
    FROM {{ mapping.starrocks_current_experiment }} ce
    JOIN document_count dc          ON dc.seq_id = ce.seq_id
                                   AND dc.cram_matches > 0
    LEFT JOIN quality_controlled qc ON qc.case_id = ce.case_id
                                   AND qc.seq_id  = ce.seq_id
    WHERE ce.case_type = 'germline'
      AND qc.seq_id IS NULL
)
SELECT pc.case_id                          AS case_id,
       pc.submitter_case_id                AS submitter_case_id,
       pc.tenant_code                      AS tenant_code,
       'germline'                          AS case_type,
       pc.project_code                     AS project_code,
       cm.role                             AS role,
       cm.affected_status                  AS affected_status,
       cm.patient_id                       AS patient_id,
       cm.sex                              AS sex,
       ce.seq_id                           AS seq_id,
       ce.aliquot                          AS aliquot,
       ce.strategy                         AS strategy,
       ca.task_id                          AS current_alignment_task_id,
       COALESCE(dc.cram_matches, 0)        AS cram_matches,
       COALESCE(dc.dragen_metrics_matches, 0) AS dragen_metrics_matches,
       qc.seq_id IS NOT NULL               AS annotated,
       ac.case_id IS NOT NULL              AS case_annotated
FROM {{ mapping.starrocks_pipeline_case }} pc
JOIN candidate cd                                        ON cd.case_id = pc.case_id
JOIN {{ mapping.starrocks_case_member }} cm              ON cm.case_id = pc.case_id
-- LEFT: a member with no experiment, alignment or CRAM must come back, so its reason is decided.
LEFT JOIN {{ mapping.starrocks_current_experiment }} ce  ON ce.case_id = pc.case_id
                                                        AND ce.patient_id = cm.patient_id
LEFT JOIN {{ mapping.starrocks_current_alignment }} ca   ON ca.seq_id = ce.seq_id
                                                        AND ca.task_type = 'alignment_germline_variant_calling'
LEFT JOIN document_count dc                              ON dc.seq_id = ce.seq_id
LEFT JOIN quality_controlled qc                          ON qc.case_id = pc.case_id
                                                        AND qc.seq_id  = ce.seq_id
-- Quality-controlled before, whatever the experiment: tells a case's first run from a later one.
LEFT JOIN (SELECT DISTINCT case_id FROM quality_controlled) ac ON ac.case_id = pc.case_id
WHERE pc.case_type = 'germline'
ORDER BY pc.case_id, cm.patient_id
