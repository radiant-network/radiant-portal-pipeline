-- One row per (candidate germline case, family member): the member's *current* sequencing
-- experiment, the gVCF of that experiment's *current* alignment, and -- when the case
-- cannot be run -- why.
--
-- Consumed by `radiant-nextflow-postprocessing-cases` (discover_scope). Full analysis:
-- `design/SJRA-1857-nextflow-postprocessing-automation.md`.
--
-- This one statement answers two questions that must never be answered separately: which
-- cases are waiting for an annotation, and which sequencing experiment stands for each
-- member. If the first considered every experiment while the second used only one, a
-- superseded experiment would keep its case eligible for ever and the DAG would re-run it
-- every night (SJRA-1857 5.1). Splitting this into a discovery query and a members query
-- would reintroduce exactly that drift, which is why they are one query.
--
-- No tenant filter and no case filter. The clinical tables are a single shared schema
-- behind the `radiant_jdbc` catalog -- not one schema per tenant -- and `cases.id` is a
-- plain single-column identity primary key over all of it, with `tenant_code` only an
-- attribute. The tenant is therefore returned rather than asked for. `params.tenants`,
-- when set, is a *grant* list, not a scope: it marks cases the service account may not
-- write to so they never reach the pipeline, rather than narrowing what is looked at.
WITH germline_case AS (
    -- `revoke` is deliberately absent. In the field a revoked case is very often precisely
    -- a case left without an annotation on purpose, so admitting it here would resurrect
    -- every one of them nightly. `staging_external_sequencing_experiment` filters the same
    -- way for the same reason.
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
    -- Exactly one sequencing experiment per (case, member): the newest `completed` one.
    --
    -- This is the whole supersession policy. A member re-sequenced after a contamination
    -- has two experiments linked to the case; taking both would put a person in the PED
    -- twice -- silently, when that person is a parent -- and taking one here while the
    -- eligibility test below considered both would never converge. One definition, used by
    -- both, is what makes either failure impossible.
    --
    -- `completed` is a whitelist, not a `revoke` blacklist: sequencing that is not finished
    -- is not a candidate, whatever the reason it is not finished.
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
    -- The same policy, one level down: a sequencing experiment re-aligned after an error
    -- carries two alignment tasks. Taking the newest is what lets `gvcf_matches` below mean
    -- one thing instead of two -- see the note on `gvcf`.
    --
    -- Joined through `sequencing_experiment_id`, never through `task_context.case_id`:
    -- alignment tasks carry a null `case_id`, so a join on it returns nothing at all.
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
    -- Counted over ONE alignment task, which is what makes the count diagnostic. Before
    -- `current_alignment` existed, two gVCFs could mean either a legitimate re-alignment or
    -- an index document mistyped as `format_code = 'gvcf'`, and the error message had to
    -- guess. Now more than one gVCF on a single task can only be the mistyped document.
    --
    -- Selected on the document's own type fields, never on its filename: naming conventions
    -- differ between callers. The germline code is `snv` (`ssnv` is somatic); the
    -- `data_type` dictionary has no `gsnv`.
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
    -- Scoped to the case, not only to the experiment. Annotation tasks *do* carry a
    -- `case_id` -- registration knows which case it wrote to -- and that is what keeps a
    -- shared sequencing experiment honest: annotating case 1 must not make case 2 look done.
    SELECT DISTINCT tc.case_id                    AS case_id,
                    tc.sequencing_experiment_id   AS seq_id
    FROM {{ mapping.clinical_task }} t
    JOIN {{ mapping.clinical_task_context }} tc ON tc.task_id = t.id
    WHERE t.task_type_code = 'radiant_germline_annotation'
      AND tc.case_id IS NOT NULL
),
candidate AS (
    -- A case is a candidate when at least one member's current experiment has a gVCF from
    -- its current alignment and carries no annotation for that same (case, experiment).
    --
    -- Note what is deliberately *not* a candidate: a case where no member has a gVCF at all.
    -- Those were joint-called upstream, are permanently out of scope, and reporting them
    -- every night would be noise. `no_gvcf` below therefore only ever describes a member of
    -- a case that is otherwise ready to run -- which is the case worth acting on.
    SELECT DISTINCT ce.case_id AS case_id
    FROM current_experiment ce
    JOIN gvcf g          ON g.seq_id = ce.seq_id
    LEFT JOIN annotated a ON a.case_id = ce.case_id
                         AND a.seq_id  = ce.seq_id
    WHERE a.seq_id IS NULL
    {% if params.task_ids %}
      -- A manual, targeted run: only cases made candidate by these alignment tasks.
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
       -- Why this case cannot be run, or NULL. Ordered most-specific-first: a member with
       -- no experiment at all has no alignment either, and saying so would be less useful
       -- than saying it has not been sequenced yet.
       --
       -- The two `pending_*` reasons are transient by design and are not errors. A member
       -- whose newest experiment has no alignment yet makes the case *wait* rather than run
       -- against the superseded one -- an annotation that would be obsolete the moment it
       -- landed.
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
-- LEFT, all three: a member with no completed experiment, no alignment or no gVCF must come
-- back carrying its reason rather than vanishing. A family silently short one member is the
-- one failure mode worth more than the rest put together.
LEFT JOIN current_experiment ce               ON ce.case_id = gc.case_id
                                             AND ce.patient_id = p.id
LEFT JOIN current_alignment ca                ON ca.seq_id = ce.seq_id
LEFT JOIN gvcf g                              ON g.seq_id = ce.seq_id
-- On the primary key alone. `cases.project_id` is a foreign key to `project.id`, so this
-- resolves to exactly one row; `project.code` is globally unique, and a project row carries
-- a single tenant_code that need not equal the case's. Adding `AND pr.tenant_code =
-- gc.tenant_code` therefore cannot narrow a genuine ambiguity -- there is none -- and can
-- only turn a match into a NULL. project_code is what the batch PATCH looks a case up by,
-- together with submitter_case_id, and it is mandatory there.
LEFT JOIN {{ mapping.clinical_project }} pr   ON pr.id = gc.project_id
ORDER BY gc.case_id,
         CASE f.relationship_to_proband_code
              WHEN 'proband' THEN 0
              WHEN 'father'  THEN 1
              WHEN 'mother'  THEN 2
              ELSE 3
         END,
         p.id
