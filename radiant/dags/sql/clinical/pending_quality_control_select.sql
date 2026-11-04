-- One row per (candidate case, member): the member's *current* sequencing experiment, the CRAM
-- and the DRAGEN metrics tarball of that experiment's *current* alignment, and why the case
-- cannot run if it cannot.
--
-- Germline cases bring every family member and their `alignment_germline_variant_calling`. A
-- somatic case brings its proband alone, with the tumor experiment and its
-- `alignment_somatic_variant_calling`; a normal experiment of the case is ignored. The pipeline
-- never reads a tumor's CRAM, so a somatic row has NULL CRAM columns and runs on its tarball.
--
-- Consumed by `radiant-nextflow-quality-control-cases` (discover_scope). Full analysis:
-- `design/SJRA-1879-nextflow-quality-control-automation.md`, and radiant-hub
-- `docs/features/RAD-110-qc-dragen-metrics-samplesheet.md` for the tarball.
--
-- Sibling of `pending_annotation_select.sql`, deliberately *not* a shared template: the
-- current-experiment and current-alignment CTEs follow the same supersession policy (newest
-- completed experiment per member, newest alignment per experiment); the case types (both here,
-- germline there), the trigger document (CRAM here, gVCF there) and the anti-join (a
-- `quality_control_metrics` task here, an annotation there) differ. Keep the policy the same
-- when touching either.
--
-- The tenant is returned, not filtered on: clinical is one shared schema and `cases.id` is
-- unique across it. `params.excluded_tenants` is a block list, not a scope.
WITH qc_case AS (
    -- `revoke` excluded deliberately: a revoked case is often one left alone on purpose, and
    -- admitting it would resurrect every one of them nightly.
    SELECT c.id                AS case_id,
           c.submitter_case_id AS submitter_case_id,
           c.tenant_code       AS tenant_code,
           c.project_id        AS project_id,
           c.proband_id        AS proband_id,
           c.case_type_code    AS case_type,
           CASE c.case_type_code
               WHEN 'somatic' THEN 'alignment_somatic_variant_calling'
               ELSE 'alignment_germline_variant_calling'
           END                 AS alignment_type
    FROM {{ mapping.clinical_case }} c
    WHERE c.case_type_code IN ('germline', 'somatic')
      AND c.status_code NOT IN ({{ mapping.pipeline_ignored_case_statuses }})
),
current_experiment AS (
    -- The supersession policy: one `completed` experiment per (case, member), newest wins.
    SELECT case_id, patient_id, seq_id, sample_id, aliquot, strategy, alignment_type
    FROM (
        SELECT chse.case_id                  AS case_id,
               s.patient_id                  AS patient_id,
               se.id                         AS seq_id,
               s.submitter_sample_id         AS sample_id,
               se.aliquot                    AS aliquot,
               se.experimental_strategy_code AS strategy,
               qc.alignment_type             AS alignment_type,
               ROW_NUMBER() OVER (
                   PARTITION BY chse.case_id, s.patient_id
                   ORDER BY se.created_on DESC, se.id DESC
               ) AS rn
        FROM {{ mapping.clinical_case_has_sequencing_experiment }} chse
        JOIN qc_case qc                                           ON qc.case_id = chse.case_id
        JOIN {{ mapping.clinical_sequencing_experiment }} se       ON se.id = chse.sequencing_experiment_id
        JOIN {{ mapping.clinical_sample }} s                       ON s.id = se.sample_id
        WHERE se.status_code = 'completed'
          AND (qc.case_type = 'germline' OR s.histology_code = 'tumoral')
    ) ranked
    WHERE rn = 1
),
current_alignment AS (
    -- Same policy one level down: a re-aligned experiment carries two alignment tasks. Ranked per
    -- type, so that a somatic alignment never supersedes the germline one of the same experiment.
    --
    -- Joined through `sequencing_experiment_id`, never `task_context.case_id`: alignment
    -- tasks carry a null case_id, so joining on it returns an empty set, not an error.
    SELECT seq_id, task_type, task_id
    FROM (
        SELECT tc.sequencing_experiment_id AS seq_id,
               t.task_type_code            AS task_type,
               t.id                        AS task_id,
               ROW_NUMBER() OVER (
                   PARTITION BY tc.sequencing_experiment_id, t.task_type_code
                   ORDER BY t.created_on DESC, t.id DESC
               ) AS rn
        FROM {{ mapping.clinical_task }} t
        JOIN {{ mapping.clinical_task_context }} tc ON tc.task_id = t.id
        WHERE t.task_type_code IN ('alignment_germline_variant_calling', 'alignment_somatic_variant_calling')
    ) ranked
    WHERE rn = 1
),
alignment_document AS (
    -- Every output of the current alignment. Selected on the task, never on filename.
    SELECT ca.seq_id        AS seq_id,
           ca.task_type     AS task_type,
           ca.task_id       AS task_id,
           d.url            AS url,
           d.data_type_code AS data_type_code,
           d.format_code    AS format_code
    FROM current_alignment ca
    JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = ca.task_id
                                                     AND thd.type = 'output'
    JOIN {{ mapping.clinical_document }} d            ON d.id = thd.document_id
),
cram AS (
    -- The germline trigger document, counted over ONE task so that two means a mistyped document,
    -- not a re-alignment. Selected on type fields, never on filename. Germline only: a tumor's
    -- CRAM selects and excludes nothing.
    SELECT seq_id,
           task_type,
           task_id,
           MAX(url)            AS url,
           COUNT(DISTINCT url) AS matches
    FROM alignment_document
    WHERE task_type      = 'alignment_germline_variant_calling'
      AND data_type_code = 'alignment'
      AND format_code    = 'cram'
    GROUP BY seq_id, task_type, task_id
),
crai AS (
    SELECT seq_id, task_type, MAX(url) AS url
    FROM alignment_document
    WHERE task_type      = 'alignment_germline_variant_calling'
      AND data_type_code = 'alignment'
      AND format_code    = 'crai'
    GROUP BY seq_id, task_type
),
dragen_metrics AS (
    -- The DRAGEN metrics tarball the pipeline reads instead of recomputing BAM_QC / VCF_QC.
    -- Optional for germline: a member without one gets its CRAM's directory. Required for a
    -- tumor. Counted like the CRAM, so two means a mistyped document rather than a choice to make.
    SELECT seq_id,
           task_type,
           MAX(url)            AS url,
           COUNT(DISTINCT url) AS matches
    FROM alignment_document
    WHERE data_type_code = 'ssup'
      AND format_code    = 'tgz'
    GROUP BY seq_id, task_type
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
trigger_alignment AS (
    -- The alignment task a member runs on. Germline: the one that published the CRAM. Somatic:
    -- the current one, whose tarball is checked per row, so that a missing one comes back as
    -- `no_metrics` instead of vanishing.
    SELECT seq_id, task_type, task_id FROM cram
    UNION ALL
    SELECT seq_id, task_type, task_id
    FROM current_alignment
    WHERE task_type = 'alignment_somatic_variant_calling'
),
candidate AS (
    -- At least one member's current experiment has a trigger alignment and no QC task for that pair.
    SELECT DISTINCT ce.case_id AS case_id
    FROM current_experiment ce
    JOIN trigger_alignment tr       ON tr.seq_id    = ce.seq_id
                                   AND tr.task_type = ce.alignment_type
    LEFT JOIN quality_controlled qc ON qc.case_id = ce.case_id
                                   AND qc.seq_id  = ce.seq_id
    WHERE qc.seq_id IS NULL
    {% if params.task_ids %}
      -- Targeted rerun: narrows candidacy, never membership -- every member of a matched
      -- case still comes back.
      AND tr.task_id IN %(task_ids)s
    {% endif %}
),
qc_member AS (
    -- Germline: every family member. Somatic: the proband alone, whose family row, when the
    -- case has one, only gives the affected status.
    SELECT cs.case_id                     AS case_id,
           f.family_member_id             AS patient_id,
           f.relationship_to_proband_code AS role,
           f.affected_status_code         AS affected_status
    FROM qc_case cs
    JOIN {{ mapping.clinical_family }} f ON f.case_id = cs.case_id
    WHERE cs.case_type = 'germline'
    UNION ALL
    SELECT cs.case_id                                  AS case_id,
           cs.proband_id                               AS patient_id,
           'proband'                                   AS role,
           COALESCE(f.affected_status_code, 'unknown') AS affected_status
    FROM qc_case cs
    LEFT JOIN {{ mapping.clinical_family }} f ON f.case_id = cs.case_id
                                             AND f.family_member_id = cs.proband_id
    WHERE cs.case_type = 'somatic'
)
SELECT cs.case_id                      AS case_id,
       cs.submitter_case_id            AS submitter_case_id,
       cs.tenant_code                  AS tenant_code,
       cs.case_type                    AS case_type,
       pr.code                         AS project_code,
       m.role                          AS role,
       m.affected_status               AS affected_status,
       p.id                            AS patient_id,
       p.sex_code                      AS sex,
       p.submitter_patient_id          AS submitter_patient_id,
       ce.sample_id                    AS sample_id,
       ce.seq_id                       AS seq_id,
       ce.aliquot                      AS aliquot,
       ce.strategy                     AS strategy,
       tr.task_id                      AS alignment_task_id,
       cr.url                          AS cram_url,
       COALESCE(cr.matches, 0)         AS cram_matches,
       ci.url                          AS crai_url,
       dm.url                          AS dragen_metrics_url,
       COALESCE(dm.matches, 0)         AS dragen_metrics_matches,
       -- Most specific first. The two `pending_*` reasons are transient, not errors.
       CASE
           WHEN ce.seq_id  IS NULL THEN 'pending_sequencing'
           WHEN ca.task_id IS NULL THEN 'pending_alignment'
           WHEN cs.case_type = 'germline' AND cr.matches IS NULL THEN 'no_cram'
           WHEN cr.matches > 1     THEN 'ambiguous_cram'
           WHEN dm.matches > 1     THEN 'ambiguous_dragen_metrics'
           -- No CRAM-directory fallback for a tumor: it would give a green run with an empty report.
           WHEN cs.case_type = 'somatic' AND dm.matches IS NULL THEN 'no_metrics'
           WHEN pr.code    IS NULL THEN 'no_project_code'
           {% if params.excluded_tenants %}
           WHEN cs.tenant_code IN %(excluded_tenants)s THEN 'tenant_excluded'
           {% endif %}
           ELSE NULL
       END                             AS exclusion_reason
FROM qc_case cs
JOIN candidate cd                             ON cd.case_id = cs.case_id
JOIN qc_member m                              ON m.case_id = cs.case_id
JOIN {{ mapping.clinical_patient }} p         ON p.id = m.patient_id
-- LEFT, all of them: a member with no experiment, alignment or CRAM must come back carrying
-- its reason. Each CTE has at most one row per (experiment, alignment type), so a member comes
-- back once.
LEFT JOIN current_experiment ce               ON ce.case_id = cs.case_id
                                             AND ce.patient_id = p.id
LEFT JOIN current_alignment ca                ON ca.seq_id = ce.seq_id
                                             AND ca.task_type = ce.alignment_type
LEFT JOIN trigger_alignment tr                ON tr.seq_id = ce.seq_id
                                             AND tr.task_type = ce.alignment_type
LEFT JOIN cram cr                             ON cr.seq_id = ce.seq_id
                                             AND cr.task_type = ce.alignment_type
LEFT JOIN crai ci                             ON ci.seq_id = ce.seq_id
                                             AND ci.task_type = ce.alignment_type
LEFT JOIN dragen_metrics dm                   ON dm.seq_id = ce.seq_id
                                             AND dm.task_type = ce.alignment_type
-- On the primary key alone: `project.code` is globally unique and a project's tenant_code
-- need not equal the case's.
LEFT JOIN {{ mapping.clinical_project }} pr   ON pr.id = cs.project_id
ORDER BY cs.case_id,
         CASE m.role
              WHEN 'proband' THEN 0
              WHEN 'father'  THEN 1
              WHEN 'mother'  THEN 2
              ELSE 3
         END,
         p.id
