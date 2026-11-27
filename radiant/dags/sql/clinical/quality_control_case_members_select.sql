-- One row per member of the named germline cases (`case_ids`): what the quality-control pipeline runs on.
-- The DAG decided these cases on `quality_control_facts_select.sql`. A member with no CRAM yet comes back
-- with a null `cram_url`.
WITH alignment_document AS (
    -- Every output of the current germline alignment, selected on the task, never on file name.
    SELECT ca.seq_id        AS seq_id,
           ca.task_id       AS task_id,
           d.url            AS url,
           d.data_type_code AS data_type_code,
           d.format_code    AS format_code
    FROM {{ mapping.starrocks_current_alignment }} ca
    JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = ca.task_id
                                                     AND thd.type = 'output'
    JOIN {{ mapping.clinical_document }} d            ON d.id = thd.document_id
    WHERE ca.task_type = 'alignment_germline_variant_calling'
),
document AS (
    -- Counted, so two means a mistyped document rather than a choice to make.
    SELECT seq_id,
           MAX(CASE WHEN data_type_code = 'alignment' AND format_code = 'cram' THEN url END)            AS cram_url,
           COUNT(DISTINCT CASE WHEN data_type_code = 'alignment' AND format_code = 'cram' THEN url END) AS cram_matches,
           MAX(CASE WHEN data_type_code = 'alignment' AND format_code = 'crai' THEN url END)            AS crai_url,
           -- The DRAGEN metrics tarball. Optional: a member without one gets its CRAM's directory.
           MAX(CASE WHEN data_type_code = 'ssup' AND format_code = 'tgz' THEN url END)                  AS dragen_metrics_url,
           COUNT(DISTINCT CASE WHEN data_type_code = 'ssup' AND format_code = 'tgz' THEN url END)       AS dragen_metrics_matches
    FROM alignment_document
    GROUP BY seq_id
)
SELECT pc.case_id                              AS case_id,
       pc.submitter_case_id                    AS submitter_case_id,
       pc.tenant_code                          AS tenant_code,
       'germline'                              AS case_type,
       pc.project_code                         AS project_code,
       cm.role                                 AS role,
       cm.affected_status                      AS affected_status,
       cm.patient_id                           AS patient_id,
       cm.sex                                  AS sex,
       cm.submitter_patient_id                 AS submitter_patient_id,
       ce.sample_id                            AS sample_id,
       ce.seq_id                               AS seq_id,
       ce.aliquot                              AS aliquot,
       ce.strategy                             AS strategy,
       ca.task_id                              AS current_alignment_task_id,
       ca.task_id                              AS alignment_task_id,
       d.cram_url                              AS cram_url,
       COALESCE(d.cram_matches, 0)             AS cram_matches,
       d.crai_url                              AS crai_url,
       d.dragen_metrics_url                    AS dragen_metrics_url,
       COALESCE(d.dragen_metrics_matches, 0)   AS dragen_metrics_matches
FROM {{ mapping.starrocks_pipeline_case }} pc
JOIN {{ mapping.starrocks_case_member }} cm              ON cm.case_id = pc.case_id
LEFT JOIN {{ mapping.starrocks_current_experiment }} ce  ON ce.case_id = pc.case_id
                                                        AND ce.patient_id = cm.patient_id
LEFT JOIN {{ mapping.starrocks_current_alignment }} ca   ON ca.seq_id = ce.seq_id
                                                        AND ca.task_type = 'alignment_germline_variant_calling'
LEFT JOIN document d                                     ON d.seq_id = ce.seq_id
WHERE pc.case_type = 'germline'
  AND pc.case_id IN %(case_ids)s
ORDER BY pc.case_id, cm.patient_id
