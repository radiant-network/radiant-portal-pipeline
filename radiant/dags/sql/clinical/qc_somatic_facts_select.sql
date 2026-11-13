-- Somatic twin of `qc_germline_facts_select.sql`, with the same columns: one row per somatic case,
-- for its proband alone. The row carries the proband's current tumor experiment, the current
-- `alignment_somatic_variant_calling` of that experiment (the work: a tumor aligned and never
-- QC'd) and its DRAGEN metrics tarball. A normal experiment of the case is ignored, and the
-- pipeline never reads a tumor's CRAM, so the CRAM columns are NULL.
--
-- Facts only: every somatic case of `pipeline_case` comes back, and the decision is made in
-- Python by `radiant.tasks.nextflow.qc.resolve.select_cases`.
WITH current_tumor AS (
    -- The proband's current tumor experiment of each somatic case. `current_experiment` ranks a
    -- somatic case per histology, so the filter keeps the newest `completed` tumor.
    SELECT ce.case_id, ce.patient_id, ce.seq_id, ce.sample_id, ce.aliquot, ce.strategy
    FROM {{ mapping.starrocks_current_experiment }} ce
    JOIN {{ mapping.starrocks_pipeline_case }} pc ON pc.case_id = ce.case_id
                                                AND pc.proband_id = ce.patient_id
    WHERE ce.case_type = 'somatic'
      AND ce.histology_code = 'tumoral'
),
current_alignment AS (
    -- The current somatic alignment of each experiment (view `current_alignment`, RAD-287).
    SELECT seq_id, task_id
    FROM {{ mapping.starrocks_current_alignment }}
    WHERE task_type = 'alignment_somatic_variant_calling'
),
dragen_metrics AS (
    -- The DRAGEN metrics tarball of the current alignment. Required: a tumor has no CRAM
    -- directory to fall back on. Counted, so two means a mistyped document rather than a choice.
    -- Selected on type fields, never on filename.
    SELECT ca.seq_id                  AS seq_id,
           MAX(d.url)                 AS url,
           COUNT(DISTINCT d.url)      AS matches
    FROM current_alignment ca
    JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = ca.task_id
                                                     AND thd.type = 'output'
    JOIN {{ mapping.clinical_document }} d            ON d.id = thd.document_id
    WHERE d.data_type_code = 'ssup'
      AND d.format_code    = 'tgz'
    GROUP BY ca.seq_id
),
quality_controlled AS (
    -- Scoped to the case, not just the experiment: QC tasks carry a case_id, and a shared
    -- experiment QC'd under case 1 must not make case 2 look done.
    SELECT DISTINCT tc.case_id                    AS case_id,
                    tc.sequencing_experiment_id   AS seq_id
    FROM {{ mapping.clinical_task }} t
    JOIN {{ mapping.clinical_task_context }} tc ON tc.task_id = t.id
    WHERE t.task_type_code = 'quality_control_metrics'
      AND tc.case_id IS NOT NULL
)
{% if live_only is defined and live_only %},
-- The scheduler's discovery reads the live cases only: work not done yet, a status still moving, or a last
-- state that is not settled (`done`, `no_trigger_document`). A settled case keeps its state in
-- `case_pipeline_state` until its facts give it work again.
settled_case AS (
    SELECT case_id
    FROM {{ mapping.starrocks_case_pipeline_state }}
    WHERE pipeline = 'qc'
      AND scope = 'case'
    GROUP BY case_id
    HAVING SUM(CASE WHEN decision = 'done' OR reason = 'no_trigger_document' THEN 0 ELSE 1 END) = 0
),
live_case AS (
    SELECT ce.case_id
    FROM current_tumor ce
    JOIN current_alignment ca       ON ca.seq_id = ce.seq_id
    LEFT JOIN quality_controlled qc ON qc.case_id = ce.case_id
                                   AND qc.seq_id = ce.seq_id
    WHERE qc.seq_id IS NULL
    UNION
    SELECT pc.case_id
    FROM {{ mapping.starrocks_pipeline_case }} pc
    LEFT JOIN settled_case s ON s.case_id = pc.case_id
    WHERE pc.status_code IN ('submitted', 'processing')
       OR s.case_id IS NULL
)
{% endif %}
SELECT pc.case_id                                  AS case_id,
       pc.submitter_case_id                        AS submitter_case_id,
       pc.tenant_code                              AS tenant_code,
       'somatic'                                   AS case_type,
       pc.project_code                             AS project_code,
       'proband'                                   AS role,
       -- The family row, when the case has one, only gives the affected status.
       COALESCE(cm.affected_status, 'unknown')     AS affected_status,
       p.id                                        AS patient_id,
       p.sex_code                                  AS sex,
       p.submitter_patient_id                      AS submitter_patient_id,
       ce.sample_id                                AS sample_id,
       ce.seq_id                                   AS seq_id,
       ce.aliquot                                  AS aliquot,
       ce.strategy                                 AS strategy,
       ca.task_id                                  AS alignment_task_id,
       CAST(NULL AS VARCHAR)                       AS cram_url,
       0                                           AS cram_matches,
       CAST(NULL AS VARCHAR)                       AS crai_url,
       dm.url                                      AS dragen_metrics_url,
       COALESCE(dm.matches, 0)                     AS dragen_metrics_matches,
       qc.seq_id IS NOT NULL                       AS annotated
FROM {{ mapping.starrocks_pipeline_case }} pc
-- The patient directly, not through `case_member`: a somatic case can have no family row.
JOIN {{ mapping.clinical_patient }} p            ON p.id = pc.proband_id
-- LEFT: a tumor not sequenced or not aligned yet comes back, with nothing to QC.
LEFT JOIN current_tumor ce                       ON ce.case_id = pc.case_id
LEFT JOIN current_alignment ca                   ON ca.seq_id = ce.seq_id
LEFT JOIN dragen_metrics dm                      ON dm.seq_id = ce.seq_id
LEFT JOIN quality_controlled qc                  ON qc.case_id = pc.case_id
                                                AND qc.seq_id  = ce.seq_id
LEFT JOIN {{ mapping.starrocks_case_member }} cm ON cm.case_id = pc.case_id
                                                AND cm.patient_id = pc.proband_id
WHERE pc.case_type = 'somatic'
{% if live_only is defined and live_only %}
  AND pc.case_id IN (SELECT case_id FROM live_case)
{% endif %}
{% if case_ids is defined and case_ids %}
  AND pc.case_id IN %(case_ids)s
{% endif %}
ORDER BY pc.case_id,
         p.id
