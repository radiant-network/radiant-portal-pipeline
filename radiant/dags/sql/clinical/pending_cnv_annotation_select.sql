-- One row per (candidate germline case, family member): the member's *current* sequencing
-- experiment, the germline CNV VCF (and CRAM, when there is one) of that experiment's
-- *current* alignment, and why the case cannot run if it cannot.
--
-- Consumed by `radiant-nextflow-cnv-postprocessing-cases` (discover_scope). Full analysis:
-- `design/cnv-post-processing-nextflow-integration.md`.
--
-- Sibling of `pending_annotation_select.sql`, deliberately *not* a shared template: the
-- current-experiment and current-alignment CTEs are the same supersession policy
-- (SJRA-1698 5), but the trigger document (a `gcnv` VCF here, a gVCF there) and the anti-join
-- (a `radiant_germline_cnv_annotation` task here, a `radiant_germline_annotation` there)
-- differ, and the unit tests render each file standalone. Keep the `current_*` CTEs identical
-- across the three `pending_*` templates when touching any of them.
--
-- The CRAM is carried, not required: the pipeline's depth-based genotype refinement reads
-- every member's alignment, and skips the step for a family missing one. Only the CNV VCF
-- decides eligibility.
--
-- The tenant is returned, not filtered on: clinical is one shared schema and `cases.id` is
-- unique across it. `params.tenants` is a grant list, not a scope.
WITH germline_case AS (
    -- `revoke` excluded deliberately: a revoked case is often one left alone on purpose, and
    -- admitting it would resurrect every one of them nightly.
    SELECT c.id                AS case_id,
           c.submitter_case_id AS submitter_case_id,
           c.tenant_code       AS tenant_code,
           c.project_id        AS project_id
    FROM {{ mapping.clinical_case }} c
    WHERE c.case_type_code = 'germline'
      AND c.status_code IN ('in_progress', 'completed')
),
current_experiment AS (
    -- The supersession policy: one `completed` experiment per (case, member), newest wins.
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
        JOIN germline_case gc                                     ON gc.case_id = chse.case_id
        JOIN {{ mapping.clinical_sequencing_experiment }} se       ON se.id = chse.sequencing_experiment_id
        JOIN {{ mapping.clinical_sample }} s                       ON s.id = se.sample_id
        WHERE se.status_code = 'completed'
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
        WHERE t.task_type_code = 'alignment_germline_variant_calling'
    ) ranked
    WHERE rn = 1
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
    FROM current_experiment ce
    JOIN gcnv g               ON g.seq_id = ce.seq_id
    LEFT JOIN cnv_annotated a ON a.case_id = ce.case_id
                             AND a.seq_id  = ce.seq_id
    WHERE a.seq_id IS NULL
    {% if params.task_ids %}
      -- Targeted rerun: narrows candidacy, never membership -- every member of a matched
      -- case still comes back.
      AND g.task_id IN %(task_ids)s
    {% endif %}
)
SELECT gc.case_id                      AS case_id,
       gc.submitter_case_id            AS submitter_case_id,
       gc.tenant_code                  AS tenant_code,
       pr.code                         AS project_code,
       f.relationship_to_proband_code  AS role,
       f.affected_status_code          AS affected_status,
       p.id                            AS patient_id,
       p.sex_code                      AS sex,
       p.submitter_patient_id          AS submitter_patient_id,
       ce.sample_id                    AS sample_id,
       ce.seq_id                       AS seq_id,
       ce.aliquot                      AS aliquot,
       ce.strategy                     AS strategy,
       g.task_id                       AS alignment_task_id,
       -- The caller, so a non-DRAGEN alignment is excluded here rather than failing hours
       -- in: the pipeline's normalization step only implements DRAGEN's CNV conventions.
       at.pipeline_name                AS alignment_pipeline,
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
           WHEN pr.code    IS NULL THEN 'no_project_code'
           {% if params.tenants %}
           WHEN gc.tenant_code NOT IN %(tenants)s THEN 'tenant_not_granted'
           {% endif %}
           ELSE NULL
       END                             AS exclusion_reason
FROM germline_case gc
JOIN candidate cd                             ON cd.case_id = gc.case_id
JOIN {{ mapping.clinical_family }} f          ON f.case_id = gc.case_id
JOIN {{ mapping.clinical_patient }} p         ON p.id = f.family_member_id
-- LEFT, all of them: a member with no experiment, alignment or CNV VCF must come back
-- carrying its reason. A family silently short one member is the worst failure available.
LEFT JOIN current_experiment ce               ON ce.case_id = gc.case_id
                                             AND ce.patient_id = p.id
LEFT JOIN current_alignment ca                ON ca.seq_id = ce.seq_id
LEFT JOIN {{ mapping.clinical_task }} at      ON at.id = ca.task_id
LEFT JOIN gcnv g                              ON g.seq_id = ce.seq_id
LEFT JOIN cram cr                             ON cr.seq_id = ce.seq_id
LEFT JOIN crai ci                             ON ci.seq_id = ce.seq_id
-- On the primary key alone: `project.code` is globally unique and a project's tenant_code
-- need not equal the case's.
LEFT JOIN {{ mapping.clinical_project }} pr   ON pr.id = gc.project_id
ORDER BY gc.case_id,
         CASE f.relationship_to_proband_code
              WHEN 'proband' THEN 0
              WHEN 'father'  THEN 1
              WHEN 'mother'  THEN 2
              ELSE 3
         END,
         p.id
