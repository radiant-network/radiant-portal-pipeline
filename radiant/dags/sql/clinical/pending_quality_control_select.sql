-- One row per (candidate germline case, family member): the member's *current* sequencing
-- experiment, the CRAM and the DRAGEN metrics tarball of that experiment's *current*
-- alignment, and why the case cannot run if it cannot.
--
-- Consumed by `radiant-nextflow-quality-control-cases` (discover_germline). Full analysis:
-- `design/SJRA-1879-nextflow-quality-control-automation.md`, and radiant-hub
-- `docs/features/RAD-110-qc-dragen-metrics-samplesheet.md` for the tarball.
--
-- Sibling of `pending_annotation_select.sql`: both read the supersession policy (SJRA-1698 5)
-- from the views `current_experiment` and `current_alignment`, and the cases from
-- `pipeline_case` (RAD-287). What differs is the trigger document (CRAM here, gVCF there) and
-- the anti-join (a `quality_control_metrics` task here, an annotation there).
--
-- The tenant is returned, not filtered on: clinical is one shared schema and `cases.id` is
-- unique across it. `params.excluded_tenants` is a block list, not a scope.
WITH current_alignment AS (
    -- The current germline alignment of each experiment (view `current_alignment`, RAD-287).
    SELECT seq_id, task_id
    FROM {{ mapping.starrocks_current_alignment }}
    WHERE task_type = 'alignment_germline_variant_calling'
),
alignment_document AS (
    -- Every output of the current alignment. Selected on the task, never on filename.
    SELECT ca.seq_id        AS seq_id,
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
    -- The trigger document, counted over ONE task so that two means a mistyped document,
    -- not a re-alignment. Selected on type fields, never on filename.
    SELECT seq_id,
           task_id,
           MAX(url)            AS url,
           COUNT(DISTINCT url) AS matches
    FROM alignment_document
    WHERE data_type_code = 'alignment'
      AND format_code    = 'cram'
    GROUP BY seq_id, task_id
),
crai AS (
    SELECT seq_id, MAX(url) AS url
    FROM alignment_document
    WHERE data_type_code = 'alignment'
      AND format_code    = 'crai'
    GROUP BY seq_id
),
dragen_metrics AS (
    -- The DRAGEN metrics tarball the pipeline reads instead of recomputing BAM_QC / VCF_QC.
    -- Optional: a member without one gets its CRAM's directory. Counted like the CRAM, so two
    -- means a mistyped document rather than a choice to make.
    SELECT seq_id,
           MAX(url)            AS url,
           COUNT(DISTINCT url) AS matches
    FROM alignment_document
    WHERE data_type_code = 'ssup'
      AND format_code    = 'tgz'
    GROUP BY seq_id
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
    -- At least one member's current experiment has a CRAM and no QC task for that pair.
    SELECT DISTINCT ce.case_id AS case_id
    FROM {{ mapping.starrocks_current_experiment }} ce
    JOIN cram cr                    ON cr.seq_id = ce.seq_id
    LEFT JOIN quality_controlled qc ON qc.case_id = ce.case_id
                                   AND qc.seq_id  = ce.seq_id
    WHERE ce.case_type = 'germline'
      AND qc.seq_id IS NULL
    {% if params.task_ids %}
      -- Targeted rerun: narrows candidacy, never membership -- every member of a matched
      -- case still comes back.
      AND cr.task_id IN %(task_ids)s
    {% endif %}
)
SELECT pc.case_id                      AS case_id,
       pc.submitter_case_id            AS submitter_case_id,
       pc.tenant_code                  AS tenant_code,
       'germline'                      AS case_type,
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
       cr.task_id                      AS alignment_task_id,
       cr.url                          AS cram_url,
       COALESCE(cr.matches, 0)         AS cram_matches,
       ci.url                          AS crai_url,
       dm.url                          AS dragen_metrics_url,
       COALESCE(dm.matches, 0)         AS dragen_metrics_matches,
       -- Most specific first. The two `pending_*` reasons are transient, not errors.
       CASE
           WHEN ce.seq_id  IS NULL THEN 'pending_sequencing'
           WHEN ca.task_id IS NULL THEN 'pending_alignment'
           WHEN cr.matches IS NULL THEN 'no_cram'
           WHEN cr.matches > 1     THEN 'ambiguous_cram'
           WHEN dm.matches > 1     THEN 'ambiguous_dragen_metrics'
           WHEN pc.project_code IS NULL THEN 'no_project_code'
           {% if params.excluded_tenants %}
           WHEN pc.tenant_code IN %(excluded_tenants)s THEN 'tenant_excluded'
           {% endif %}
           ELSE NULL
       END                             AS exclusion_reason
FROM {{ mapping.starrocks_pipeline_case }} pc
JOIN candidate cd                                    ON cd.case_id = pc.case_id
JOIN {{ mapping.starrocks_case_member }} cm          ON cm.case_id = pc.case_id
-- LEFT, all of them: a member with no experiment, alignment or CRAM must come back carrying
-- its reason. Each CTE has at most one row per experiment, so a member comes back once.
LEFT JOIN {{ mapping.starrocks_current_experiment }} ce ON ce.case_id = pc.case_id
                                                    AND ce.patient_id = cm.patient_id
LEFT JOIN current_alignment ca                       ON ca.seq_id = ce.seq_id
LEFT JOIN cram cr                                    ON cr.seq_id = ce.seq_id
LEFT JOIN crai ci                                    ON ci.seq_id = ce.seq_id
LEFT JOIN dragen_metrics dm                          ON dm.seq_id = ce.seq_id
WHERE pc.case_type = 'germline'
ORDER BY pc.case_id,
         CASE cm.role
              WHEN 'proband' THEN 0
              WHEN 'father'  THEN 1
              WHEN 'mother'  THEN 2
              ELSE 3
         END,
         cm.patient_id
