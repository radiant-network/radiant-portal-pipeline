-- One row per (candidate germline case, family member): the member's *current* sequencing
-- experiment, the gVCF of that experiment's *current* alignment, and why the case cannot
-- run if it cannot.
--
-- Consumed by `radiant-nextflow-postprocessing-cases` (discover_scope). Full analysis:
-- `design/SJRA-1698-nextflow-postprocessing-automation.md`.
--
-- One statement on purpose: it decides both which cases are pending and which experiment
-- stands for each member. Split in two, those definitions drift and a superseded experiment
-- keeps its case eligible for ever (SJRA-1698 5.1).
--
-- The tenant is returned, not filtered on: clinical is one shared schema and `cases.id` is
-- unique across it. `params.tenants` is a grant list, not a scope -- it marks cases the
-- service account may not write to, rather than narrowing what is looked at.
WITH germline_case AS (
    -- `revoke` excluded deliberately: a revoked case is often one left un-annotated on
    -- purpose, and admitting it would resurrect every one of them nightly.
    SELECT c.id                AS case_id,
           c.submitter_case_id AS submitter_case_id,
           c.primary_condition AS primary_condition,
           c.tenant_code       AS tenant_code,
           c.project_id        AS project_id
    FROM {{ mapping.clinical_case }} c
    WHERE c.case_type_code = 'germline'
      AND c.status_code IN ('in_progress', 'completed')
),
current_experiment AS (
    -- The supersession policy: one `completed` experiment per (case, member), newest wins.
    -- A re-sequenced member otherwise appears twice -- silently, when that member is a
    -- parent. `completed` is a whitelist: unfinished sequencing is not a candidate.
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
gvcf AS (
    -- Counted over ONE task, which is what makes the count diagnostic: across every
    -- alignment two gVCFs could be a re-alignment or a mistyped index, and the error had to
    -- guess. Over the current task alone it can only be the mistyped document.
    --
    -- Selected on the document's type fields, never its filename -- conventions differ
    -- between callers. Germline is `snv` (`ssnv` is somatic); there is no `gsnv`.
    SELECT ca.seq_id             AS seq_id,
           ca.task_id            AS task_id,
           MAX(d.url)            AS url,
           COUNT(DISTINCT d.url) AS matches
    FROM current_alignment ca
    JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = ca.task_id
                                                     AND thd.type = 'output'
    JOIN {{ mapping.clinical_document }} d            ON d.id = thd.document_id
                                                     AND d.data_type_code = 'snv'
                                                     AND d.format_code    = 'gvcf'
    GROUP BY ca.seq_id, ca.task_id
),
annotated AS (
    -- Scoped to the case, not just the experiment. Annotation tasks do carry a case_id, and
    -- that is what keeps a shared experiment honest: annotating case 1 must not make case 2
    -- look done.
    SELECT DISTINCT tc.case_id                    AS case_id,
                    tc.sequencing_experiment_id   AS seq_id
    FROM {{ mapping.clinical_task }} t
    JOIN {{ mapping.clinical_task_context }} tc ON tc.task_id = t.id
    WHERE t.task_type_code = 'radiant_germline_annotation'
      AND tc.case_id IS NOT NULL
),
candidate AS (
    -- At least one member's current experiment has a gVCF and no annotation for that pair.
    --
    -- A case where *no* member has a gVCF is deliberately not a candidate: joint-called
    -- upstream, permanently out of scope, and reporting it nightly would be noise. So
    -- `no_gvcf` below only ever describes a member of a case that is otherwise ready.
    SELECT DISTINCT ce.case_id AS case_id
    FROM current_experiment ce
    JOIN gvcf g          ON g.seq_id = ce.seq_id
    LEFT JOIN annotated a ON a.case_id = ce.case_id
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
       gc.primary_condition            AS primary_condition,
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
       g.url                           AS gvcf_url,
       COALESCE(g.matches, 0)          AS gvcf_matches,
       -- Most specific first: a member with no experiment has no alignment either, and
       -- saying so is less useful. The two `pending_*` reasons are transient, not errors --
       -- they make the case wait rather than run against a superseded experiment.
       CASE
           WHEN ce.seq_id  IS NULL THEN 'pending_sequencing'
           WHEN ca.task_id IS NULL THEN 'pending_alignment'
           WHEN g.matches  IS NULL THEN 'no_gvcf'
           WHEN g.matches > 1      THEN 'ambiguous_gvcf'
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
-- LEFT, all three: a member with no experiment, alignment or gVCF must come back carrying
-- its reason. A family silently short one member is the worst failure available here.
LEFT JOIN current_experiment ce               ON ce.case_id = gc.case_id
                                             AND ce.patient_id = p.id
LEFT JOIN current_alignment ca                ON ca.seq_id = ce.seq_id
LEFT JOIN gvcf g                              ON g.seq_id = ce.seq_id
-- On the primary key alone. `project.code` is globally unique and a project's tenant_code
-- need not equal the case's, so `AND pr.tenant_code = gc.tenant_code` could only turn a
-- match into a NULL. The batch PATCH looks a case up by (project_code, submitter_case_id).
LEFT JOIN {{ mapping.clinical_project }} pr   ON pr.id = gc.project_id
ORDER BY gc.case_id,
         CASE f.relationship_to_proband_code
              WHEN 'proband' THEN 0
              WHEN 'father'  THEN 1
              WHEN 'mother'  THEN 2
              ELSE 3
         END,
         p.id
