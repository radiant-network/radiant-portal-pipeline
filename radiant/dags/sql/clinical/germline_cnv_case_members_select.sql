-- One row per member of the named germline cases (`case_ids`): what the CNV post-processing pipeline runs on.
-- The DAG decided these cases on `germline_cnv_facts_select.sql`; both read the CNV VCF from the view
-- `current_germline_gcnv`. A member with no CNV VCF yet comes back with a null `gcnv_url`.
--
-- The CRAM is carried, not required: the pipeline's depth-based genotype refinement reads every member's
-- alignment, and skips the step for a family missing one (`CnvFamily.crams_complete`).
WITH alignment_document AS (
    -- Every output of the current germline alignment, selected on the task, never on file name.
    SELECT ca.seq_id        AS seq_id,
           d.url            AS url,
           d.format_code    AS format_code
    FROM {{ mapping.starrocks_current_alignment }} ca
    JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = ca.task_id
                                                     AND thd.type = 'output'
    JOIN {{ mapping.clinical_document }} d            ON d.id = thd.document_id
                                                     AND d.data_type_code = 'alignment'
    WHERE ca.task_type = 'alignment_germline_variant_calling'
),
cram AS (
    SELECT seq_id,
           MAX(CASE WHEN format_code = 'cram' THEN url END) AS cram_url,
           MAX(CASE WHEN format_code = 'crai' THEN url END) AS crai_url
    FROM alignment_document
    GROUP BY seq_id
)
SELECT pc.case_id                      AS case_id,
       pc.submitter_case_id            AS submitter_case_id,
       pc.tenant_code                  AS tenant_code,
       pc.project_code                 AS project_code,
       cm.role                         AS role,
       cm.affected_status              AS affected_status,
       cm.patient_id                   AS patient_id,
       cm.sex                          AS sex,
       cm.submitter_patient_id         AS submitter_patient_id,
       ce.sample_id                    AS sample_id,
       ce.seq_id                       AS seq_id,
       ce.aliquot                      AS aliquot,
       ce.strategy                     AS strategy,
       ca.task_id                      AS current_alignment_task_id,
       ca.pipeline_name                AS alignment_pipeline,
       g.task_id                       AS alignment_task_id,
       g.url                           AS gcnv_url,
       COALESCE(g.matches, 0)          AS gcnv_matches,
       cr.cram_url                     AS cram_url,
       cr.crai_url                     AS crai_url
FROM {{ mapping.starrocks_pipeline_case }} pc
JOIN {{ mapping.starrocks_case_member }} cm              ON cm.case_id = pc.case_id
LEFT JOIN {{ mapping.starrocks_current_experiment }} ce  ON ce.case_id = pc.case_id
                                                        AND ce.patient_id = cm.patient_id
LEFT JOIN {{ mapping.starrocks_current_alignment }} ca   ON ca.seq_id = ce.seq_id
                                                        AND ca.task_type = 'alignment_germline_variant_calling'
LEFT JOIN {{ mapping.starrocks_current_germline_gcnv }} g ON g.seq_id = ce.seq_id
LEFT JOIN cram cr                                        ON cr.seq_id = ce.seq_id
WHERE pc.case_type = 'germline'
  AND pc.case_id IN %(case_ids)s
ORDER BY pc.case_id, cm.patient_id
