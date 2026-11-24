-- One row per candidate somatic case, for its proband's current tumor: the facts the case scheduler decides on
-- (`decide_somatic_cases`, radiant/tasks/nextflow/qc/resolve.py). No decision here.
--
-- A candidate is a somatic case whose proband's current tumor experiment has a current
-- `alignment_somatic_variant_calling` and no `quality_control_metrics` task for the case. A normal experiment is
-- ignored: the pipeline reads the tumor's DRAGEN metrics tarball alone, its trigger document.
WITH current_tumor AS (
    -- `current_experiment` ranks a somatic case per histology, so the filter keeps the newest `completed` tumor.
    SELECT ce.case_id, ce.seq_id, ce.aliquot, ce.strategy
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
    -- Counted, so two means a mistyped document rather than a choice. Selected on type fields, never on file name.
    SELECT ca.seq_id             AS seq_id,
           COUNT(DISTINCT d.url) AS matches
    FROM current_alignment ca
    JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = ca.task_id
                                                     AND thd.type = 'output'
    JOIN {{ mapping.clinical_document }} d            ON d.id = thd.document_id
    WHERE d.data_type_code = 'ssup'
      AND d.format_code    = 'tgz'
    GROUP BY ca.seq_id
),
quality_controlled AS (
    SELECT DISTINCT tc.case_id                  AS case_id,
                    tc.sequencing_experiment_id AS seq_id
    FROM {{ mapping.clinical_task }} t
    JOIN {{ mapping.clinical_task_context }} tc ON tc.task_id = t.id
    WHERE t.task_type_code = 'quality_control_metrics'
      AND tc.case_id IS NOT NULL
)
SELECT pc.case_id                              AS case_id,
       pc.submitter_case_id                    AS submitter_case_id,
       pc.tenant_code                          AS tenant_code,
       'somatic'                               AS case_type,
       pc.project_code                         AS project_code,
       'proband'                               AS role,
       -- The family row, when the case has one, only gives the affected status.
       COALESCE(cm.affected_status, 'unknown') AS affected_status,
       p.id                                    AS patient_id,
       p.sex_code                              AS sex,
       ce.seq_id                               AS seq_id,
       ce.aliquot                              AS aliquot,
       ce.strategy                             AS strategy,
       ca.task_id                              AS current_alignment_task_id,
       -- A tumor's CRAM plays no role: the same columns as the germline facts.
       0                                       AS cram_matches,
       COALESCE(dm.matches, 0)                 AS dragen_metrics_matches,
       FALSE                                   AS annotated,
       ac.case_id IS NOT NULL                  AS case_annotated
FROM {{ mapping.starrocks_pipeline_case }} pc
-- The patient directly, not through `case_member`: a somatic case can have no family row.
JOIN {{ mapping.clinical_patient }} p            ON p.id = pc.proband_id
JOIN current_tumor ce                            ON ce.case_id = pc.case_id
JOIN current_alignment ca                        ON ca.seq_id = ce.seq_id
LEFT JOIN dragen_metrics dm                      ON dm.seq_id = ce.seq_id
LEFT JOIN quality_controlled qc                  ON qc.case_id = pc.case_id
                                                AND qc.seq_id  = ce.seq_id
-- Quality-controlled before, whatever the experiment: tells a case's first run from a later one.
LEFT JOIN (SELECT DISTINCT case_id FROM quality_controlled) ac ON ac.case_id = pc.case_id
LEFT JOIN {{ mapping.starrocks_case_member }} cm ON cm.case_id = pc.case_id
                                                AND cm.patient_id = pc.proband_id
WHERE pc.case_type = 'somatic'
  AND qc.seq_id IS NULL
ORDER BY pc.case_id
