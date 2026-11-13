-- One row per (candidate germline case, family member): the member's *current* sequencing
-- experiment, the germline CNV VCF (and CRAM, when there is one) of that experiment's
-- *current* alignment, and why the case cannot run if it cannot.
--
-- Consumed by `radiant-nextflow-cnv-postprocessing-cases` (discover_scope). Full analysis:
-- `design/cnv-post-processing-nextflow-integration.md`.
--
-- Sibling of `pending_annotation_select.sql`: both read the supersession policy (SJRA-1698 5)
-- from the views `current_experiment` and `current_alignment`, and the cases from
-- `pipeline_case` (RAD-287). What differs is the trigger document (a `gcnv` VCF here, a gVCF
-- there) and the anti-join (a `radiant_germline_cnv_annotation` task here, a
-- `radiant_germline_annotation` there).
--
-- The CRAM is carried, not required: the pipeline's depth-based genotype refinement reads
-- every member's alignment, and skips the step for a family missing one. Only the CNV VCF
-- decides eligibility.
--
-- The tenant is returned, not filtered on: clinical is one shared schema and `cases.id` is
-- unique across it. `params.excluded_tenants` is a block list, not a scope.
WITH current_alignment AS (
    -- The current germline alignment of each experiment (view `current_alignment`, RAD-287).
    SELECT seq_id, task_id, pipeline_name
    FROM {{ mapping.starrocks_current_alignment }}
    WHERE task_type = 'alignment_germline_variant_calling'
),
alignment_document AS (
    -- Every output of the current alignment, selected on the task, never on filename.
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
gcnv AS (
    -- The trigger document, counted over ONE task so that two means a mistyped document
    -- (typically the index recorded as `vcf` rather than `tbi`), not a re-alignment.
    -- Selected on type fields, never on filename: `gcnv` is germline (`scnv` is somatic).
    SELECT seq_id,
           task_id,
           MAX(url)            AS url,
           COUNT(DISTINCT url) AS matches
    FROM alignment_document
    WHERE data_type_code = 'gcnv'
      AND format_code    = 'vcf'
    GROUP BY seq_id, task_id
),
cram AS (
    -- Optional input to the pipeline's depth-based genotype refinement. Not a reason to
    -- exclude: Python decides per family whether every member has one.
    SELECT seq_id, MAX(url) AS url
    FROM alignment_document
    WHERE data_type_code = 'alignment'
      AND format_code    = 'cram'
    GROUP BY seq_id
),
crai AS (
    SELECT seq_id, MAX(url) AS url
    FROM alignment_document
    WHERE data_type_code = 'alignment'
      AND format_code    = 'crai'
    GROUP BY seq_id
),
cnv_annotated AS (
    -- Scoped to the case, not just the experiment: CNV annotation tasks carry a case_id,
    -- and a shared experiment annotated under case 1 must not make case 2 look done.
    SELECT DISTINCT tc.case_id                    AS case_id,
                    tc.sequencing_experiment_id   AS seq_id
    FROM {{ mapping.clinical_task }} t
    JOIN {{ mapping.clinical_task_context }} tc ON tc.task_id = t.id
    WHERE t.task_type_code = 'radiant_germline_cnv_annotation'
      AND tc.case_id IS NOT NULL
),
candidate AS (
    -- At least one member's current experiment has a CNV VCF and no CNV annotation for that
    -- pair. A case where *no* member has one is deliberately not a candidate: joint-called
    -- upstream, out of scope, and reporting it nightly would be noise.
    SELECT DISTINCT ce.case_id AS case_id
    FROM {{ mapping.starrocks_current_experiment }} ce
    JOIN gcnv g               ON g.seq_id = ce.seq_id
    LEFT JOIN cnv_annotated a ON a.case_id = ce.case_id
                             AND a.seq_id  = ce.seq_id
    WHERE ce.case_type = 'germline'
      AND a.seq_id IS NULL
    {% if params.task_ids %}
      -- Targeted rerun: narrows candidacy, never membership -- every member of a matched
      -- case still comes back.
      AND g.task_id IN %(task_ids)s
    {% endif %}
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
       g.task_id                       AS alignment_task_id,
       -- The caller, so a non-DRAGEN alignment is excluded here rather than failing hours
       -- in: the pipeline's normalization step only implements DRAGEN's CNV conventions.
       ca.pipeline_name                AS alignment_pipeline,
       g.url                           AS gcnv_url,
       COALESCE(g.matches, 0)          AS gcnv_matches,
       cr.url                          AS cram_url,
       ci.url                          AS crai_url,
       -- Most specific first. The two `pending_*` reasons are transient, not errors.
       CASE
           WHEN ce.seq_id  IS NULL THEN 'pending_sequencing'
           WHEN ca.task_id IS NULL THEN 'pending_alignment'
           WHEN g.matches  IS NULL THEN 'no_gcnv'
           WHEN g.matches > 1      THEN 'ambiguous_gcnv'
           WHEN pc.project_code IS NULL THEN 'no_project_code'
           {% if params.excluded_tenants %}
           WHEN pc.tenant_code IN %(excluded_tenants)s THEN 'tenant_excluded'
           {% endif %}
           ELSE NULL
       END                             AS exclusion_reason
FROM {{ mapping.starrocks_pipeline_case }} pc
JOIN candidate cd                                    ON cd.case_id = pc.case_id
JOIN {{ mapping.starrocks_case_member }} cm          ON cm.case_id = pc.case_id
-- LEFT, all of them: a member with no experiment, alignment or CNV VCF must come back
-- carrying its reason. A family silently short one member is the worst failure available.
LEFT JOIN {{ mapping.starrocks_current_experiment }} ce ON ce.case_id = pc.case_id
                                                    AND ce.patient_id = cm.patient_id
LEFT JOIN current_alignment ca                       ON ca.seq_id = ce.seq_id
LEFT JOIN gcnv g                                     ON g.seq_id = ce.seq_id
LEFT JOIN cram cr                                    ON cr.seq_id = ce.seq_id
LEFT JOIN crai ci                                    ON ci.seq_id = ce.seq_id
WHERE pc.case_type = 'germline'
ORDER BY pc.case_id,
         CASE cm.role
              WHEN 'proband' THEN 0
              WHEN 'father'  THEN 1
              WHEN 'mother'  THEN 2
              ELSE 3
         END,
         cm.patient_id
