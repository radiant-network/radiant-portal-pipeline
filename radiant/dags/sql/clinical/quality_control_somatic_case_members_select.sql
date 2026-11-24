-- One row per named somatic case (`case_ids`), for its proband's current tumor: what the quality-control pipeline
-- runs on. The case scheduler decided these cases on `quality_control_somatic_facts_select.sql`. The pipeline never
-- reads a tumor's CRAM, so the CRAM columns are NULL; a case whose tumor is not aligned does not come back.
WITH current_tumor AS (
    SELECT ce.case_id, ce.seq_id, ce.sample_id, ce.aliquot, ce.strategy
    FROM {{ mapping.starrocks_current_experiment }} ce
    JOIN {{ mapping.starrocks_pipeline_case }} pc ON pc.case_id = ce.case_id
                                                AND pc.proband_id = ce.patient_id
    WHERE ce.case_type = 'somatic'
      AND ce.histology_code = 'tumoral'
),
current_alignment AS (
    SELECT seq_id, task_id
    FROM {{ mapping.starrocks_current_alignment }}
    WHERE task_type = 'alignment_somatic_variant_calling'
),
dragen_metrics AS (
    -- Required: a tumor has no CRAM directory to fall back on. Selected on type fields, never on file name.
    SELECT ca.seq_id             AS seq_id,
           MAX(d.url)            AS url,
           COUNT(DISTINCT d.url) AS matches
    FROM current_alignment ca
    JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = ca.task_id
                                                     AND thd.type = 'output'
    JOIN {{ mapping.clinical_document }} d            ON d.id = thd.document_id
    WHERE d.data_type_code = 'ssup'
      AND d.format_code    = 'tgz'
    GROUP BY ca.seq_id
)
SELECT pc.case_id                              AS case_id,
       pc.submitter_case_id                    AS submitter_case_id,
       pc.tenant_code                          AS tenant_code,
       'somatic'                               AS case_type,
       pc.project_code                         AS project_code,
       'proband'                               AS role,
       COALESCE(cm.affected_status, 'unknown') AS affected_status,
       p.id                                    AS patient_id,
       p.sex_code                              AS sex,
       p.submitter_patient_id                  AS submitter_patient_id,
       ce.sample_id                            AS sample_id,
       ce.seq_id                               AS seq_id,
       ce.aliquot                              AS aliquot,
       ce.strategy                             AS strategy,
       ca.task_id                              AS current_alignment_task_id,
       ca.task_id                              AS alignment_task_id,
       CAST(NULL AS VARCHAR)                   AS cram_url,
       0                                       AS cram_matches,
       CAST(NULL AS VARCHAR)                   AS crai_url,
       dm.url                                  AS dragen_metrics_url,
       COALESCE(dm.matches, 0)                 AS dragen_metrics_matches
FROM {{ mapping.starrocks_pipeline_case }} pc
-- The patient directly, not through `case_member`: a somatic case can have no family row.
JOIN {{ mapping.clinical_patient }} p            ON p.id = pc.proband_id
JOIN current_tumor ce                            ON ce.case_id = pc.case_id
JOIN current_alignment ca                        ON ca.seq_id = ce.seq_id
LEFT JOIN dragen_metrics dm                      ON dm.seq_id = ce.seq_id
LEFT JOIN {{ mapping.starrocks_case_member }} cm ON cm.case_id = pc.case_id
                                                AND cm.patient_id = pc.proband_id
WHERE pc.case_type = 'somatic'
  AND pc.case_id IN %(case_ids)s
ORDER BY pc.case_id
