-- Somatic sibling of `pending_quality_control_select.sql`, with the same output columns: one row
-- per candidate somatic case, for its proband alone. The row carries the proband's *current* tumor
-- experiment and the DRAGEN metrics tarball of that experiment's *current*
-- `alignment_somatic_variant_calling`. A normal experiment of the case is ignored, and the
-- pipeline never reads a tumor's CRAM, so the CRAM columns are NULL. Both files read the
-- supersession policy from the views `current_experiment` and `current_alignment`, and the cases
-- from `pipeline_case` (RAD-287).
--
-- The tenant is returned, not filtered on: clinical is one shared schema and `cases.id` is
-- unique across it. `params.excluded_tenants` is a block list, not a scope.
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
       -- Most specific first.
       CASE
           WHEN dm.matches > 1     THEN 'ambiguous_dragen_metrics'
           -- No CRAM-directory fallback for a tumor: it would give a green run with an empty report.
           WHEN dm.matches IS NULL THEN 'no_metrics'
           WHEN pc.project_code IS NULL THEN 'no_project_code'
           {% if params.excluded_tenants %}
           WHEN pc.tenant_code IN %(excluded_tenants)s THEN 'tenant_excluded'
           {% endif %}
           ELSE NULL
       END                                         AS exclusion_reason
FROM {{ mapping.starrocks_pipeline_case }} pc
-- The patient directly, not through `case_member`: a somatic case can have no family row.
JOIN {{ mapping.clinical_patient }} p            ON p.id = pc.proband_id
JOIN current_tumor ce                            ON ce.case_id = pc.case_id
JOIN current_alignment ca                        ON ca.seq_id = ce.seq_id
LEFT JOIN dragen_metrics dm                      ON dm.seq_id = ce.seq_id
LEFT JOIN quality_controlled qc                  ON qc.case_id = pc.case_id
                                                AND qc.seq_id  = ce.seq_id
LEFT JOIN {{ mapping.starrocks_case_member }} cm ON cm.case_id = pc.case_id
                                                AND cm.patient_id = pc.proband_id
WHERE pc.case_type = 'somatic'
  AND qc.seq_id IS NULL
{% if params.task_ids %}
  -- Targeted rerun.
  AND ca.task_id IN %(task_ids)s
{% endif %}
ORDER BY pc.case_id,
         p.id
