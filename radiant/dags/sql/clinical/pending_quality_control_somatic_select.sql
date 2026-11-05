-- Somatic sibling of `pending_quality_control_select.sql`, with the same output columns: one row
-- per candidate somatic case, for its proband alone. The row carries the proband's *current* tumor
-- experiment and the DRAGEN metrics tarball of that experiment's *current*
-- `alignment_somatic_variant_calling`. A normal experiment of the case is ignored, and the
-- pipeline never reads a tumor's CRAM, so the CRAM columns are NULL. Keep the supersession policy
-- the same in both files.
--
-- The tenant is returned, not filtered on: clinical is one shared schema and `cases.id` is
-- unique across it. `params.excluded_tenants` is a block list, not a scope.
WITH somatic_case AS (
    -- `revoke` excluded deliberately: a revoked case is often one left alone on purpose, and
    -- admitting it would resurrect every one of them nightly.
    SELECT c.id                AS case_id,
           c.submitter_case_id AS submitter_case_id,
           c.tenant_code       AS tenant_code,
           c.project_id        AS project_id,
           c.proband_id        AS proband_id
    FROM {{ mapping.clinical_case }} c
    WHERE c.case_type_code = 'somatic'
      AND c.status_code NOT IN ({{ mapping.pipeline_ignored_case_statuses }})
),
current_experiment AS (
    -- The supersession policy: one `completed` tumor experiment per (case, proband), newest wins.
    SELECT case_id, patient_id, seq_id, sample_id, aliquot, strategy
    FROM (
        SELECT chse.case_id                  AS case_id,
               s.patient_id                  AS patient_id,
               se.id                         AS seq_id,
               s.submitter_sample_id         AS sample_id,
               se.aliquot                    AS aliquot,
               se.experimental_strategy_code AS strategy,
               ROW_NUMBER() OVER (
                   PARTITION BY chse.case_id, s.patient_id
                   ORDER BY se.created_on DESC, se.id DESC
               ) AS rn
        FROM {{ mapping.clinical_case_has_sequencing_experiment }} chse
        JOIN somatic_case sc                                      ON sc.case_id = chse.case_id
        JOIN {{ mapping.clinical_sequencing_experiment }} se       ON se.id = chse.sequencing_experiment_id
        JOIN {{ mapping.clinical_sample }} s                       ON s.id = se.sample_id
                                                                  AND s.patient_id = sc.proband_id
        WHERE se.status_code = 'completed'
          AND s.histology_code = 'tumoral'
    ) ranked
    WHERE rn = 1
),
current_alignment AS (
    -- Same policy one level down: a re-aligned experiment carries two alignment tasks.
    --
    -- Joined through `sequencing_experiment_id`, never `task_context.case_id`: alignment
    -- tasks carry a null case_id, so joining on it returns an empty set, not an error.
    SELECT seq_id, task_id
    FROM (
        SELECT tc.sequencing_experiment_id AS seq_id,
               t.id                        AS task_id,
               ROW_NUMBER() OVER (
                   PARTITION BY tc.sequencing_experiment_id
                   ORDER BY t.created_on DESC, t.id DESC
               ) AS rn
        FROM {{ mapping.clinical_task }} t
        JOIN {{ mapping.clinical_task_context }} tc ON tc.task_id = t.id
        WHERE t.task_type_code = 'alignment_somatic_variant_calling'
    ) ranked
    WHERE rn = 1
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
),
candidate AS (
    -- The tumor experiment has an alignment and no QC task for that pair.
    SELECT DISTINCT ce.case_id AS case_id
    FROM current_experiment ce
    JOIN current_alignment ca       ON ca.seq_id = ce.seq_id
    LEFT JOIN quality_controlled qc ON qc.case_id = ce.case_id
                                   AND qc.seq_id  = ce.seq_id
    WHERE qc.seq_id IS NULL
    {% if params.task_ids %}
      -- Targeted rerun: narrows candidacy, never membership.
      AND ca.task_id IN %(task_ids)s
    {% endif %}
)
SELECT sc.case_id                                  AS case_id,
       sc.submitter_case_id                        AS submitter_case_id,
       sc.tenant_code                              AS tenant_code,
       'somatic'                                   AS case_type,
       pr.code                                     AS project_code,
       'proband'                                   AS role,
       -- The family row, when the case has one, only gives the affected status.
       COALESCE(f.affected_status_code, 'unknown') AS affected_status,
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
       -- Most specific first. The two `pending_*` reasons are transient, not errors.
       CASE
           WHEN ce.seq_id  IS NULL THEN 'pending_sequencing'
           WHEN ca.task_id IS NULL THEN 'pending_alignment'
           WHEN dm.matches > 1     THEN 'ambiguous_dragen_metrics'
           -- No CRAM-directory fallback for a tumor: it would give a green run with an empty report.
           WHEN dm.matches IS NULL THEN 'no_metrics'
           WHEN pr.code    IS NULL THEN 'no_project_code'
           {% if params.excluded_tenants %}
           WHEN sc.tenant_code IN %(excluded_tenants)s THEN 'tenant_excluded'
           {% endif %}
           ELSE NULL
       END                                         AS exclusion_reason
FROM somatic_case sc
JOIN candidate cd                             ON cd.case_id = sc.case_id
JOIN {{ mapping.clinical_patient }} p         ON p.id = sc.proband_id
LEFT JOIN {{ mapping.clinical_family }} f     ON f.case_id = sc.case_id
                                             AND f.family_member_id = sc.proband_id
-- LEFT, all of them: a proband with no tumor experiment or alignment must come back carrying
-- its reason. Each CTE has at most one row per experiment, so the proband comes back once.
LEFT JOIN current_experiment ce               ON ce.case_id = sc.case_id
                                             AND ce.patient_id = p.id
LEFT JOIN current_alignment ca                ON ca.seq_id = ce.seq_id
LEFT JOIN dragen_metrics dm                   ON dm.seq_id = ce.seq_id
-- On the primary key alone: `project.code` is globally unique and a project's tenant_code
-- need not equal the case's.
LEFT JOIN {{ mapping.clinical_project }} pr   ON pr.id = sc.project_id
ORDER BY sc.case_id,
         p.id
