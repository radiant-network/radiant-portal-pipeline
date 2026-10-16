-- One row per (candidate somatic calling task, somatic case it belongs to, sequencing experiment
-- it called): the raw somatic SNV VCF the task published, and why it cannot run if it cannot.
--
-- Consumed by `radiant-nextflow-somatic-snv-annotation-cases` (discover_scope). Design: radiant-hub
-- `docs/features/RAD-23-somatic-vep-annotation.md`.
--
-- The unit of work is the calling task, not the case: a tumor-normal and a tumor-only call on the
-- same tumor sample are two VCFs, and both are annotated. A calling task is pending on a case when
-- it published an `ssnv`/`vcf` document and no `radiant_somatic_annotation` registered on that
-- case has that document as input. Per case, not global: a registration that succeeded in one
-- tenant must not hide a case of another.
--
-- Only the `ssnv` VCF is selected: the `scnv` VCF of the same task is never a trigger.
--
-- The tenant is returned, not filtered on: clinical is one shared schema and `cases.id` is unique
-- across it. `params.excluded_tenants` is a block list, not a scope.
WITH somatic_case AS (
    -- `revoke` excluded deliberately, as in the germline queries.
    SELECT c.id                AS case_id,
           c.submitter_case_id AS submitter_case_id,
           c.tenant_code       AS tenant_code,
           c.project_id        AS project_id
    FROM {{ mapping.clinical_case }} c
    WHERE c.case_type_code = 'somatic'
      AND c.status_code NOT IN ({{ mapping.pipeline_ignored_case_statuses }})
),
task_experiment AS (
    -- DISTINCT: a case-related task type (`tumor_only_variant_calling`) has one context row per
    -- case, the others one row with a null case_id. The experiments are what matter here.
    SELECT DISTINCT tc.task_id                  AS task_id,
                    t.task_type_code            AS task_type,
                    tc.sequencing_experiment_id AS seq_id
    FROM {{ mapping.clinical_task }} t
    JOIN {{ mapping.clinical_task_context }} tc ON tc.task_id = t.id
    WHERE t.task_type_code IN (
        'somatic_variant_calling',
        'tumor_only_variant_calling',
        'alignment_somatic_variant_calling'
    )
),
task_size AS (
    SELECT task_id, COUNT(DISTINCT seq_id) AS experiments
    FROM task_experiment
    GROUP BY task_id
),
case_task AS (
    -- The somatic cases holding *every* experiment of the task. Joined through
    -- `case_has_sequencing_experiment`, never `task_context.case_id`: only
    -- `tumor_only_variant_calling` carries one. And every experiment, because the portal refuses
    -- an input document produced by an experiment the case does not hold.
    SELECT chse.case_id AS case_id,
           te.task_id   AS task_id
    FROM task_experiment te
    JOIN task_size ts                                          ON ts.task_id = te.task_id
    JOIN {{ mapping.clinical_case_has_sequencing_experiment }} chse ON chse.sequencing_experiment_id = te.seq_id
    JOIN somatic_case sc                                       ON sc.case_id = chse.case_id
    GROUP BY chse.case_id, te.task_id, ts.experiments
    HAVING COUNT(DISTINCT te.seq_id) = ts.experiments
),
annotation_context AS (
    -- Every somatic annotation, with the case it was registered on. The type is case-related in
    -- the portal, so its context always carries the case.
    SELECT DISTINCT t.id       AS annotation_id,
                    tc.case_id AS case_id
    FROM {{ mapping.clinical_task }} t
    JOIN {{ mapping.clinical_task_context }} tc ON tc.task_id = t.id
    WHERE t.task_type_code = 'radiant_somatic_annotation'
      AND tc.case_id IS NOT NULL
),
annotation_input AS (
    SELECT DISTINCT thd.task_id     AS annotation_id,
                    thd.document_id AS document_id
    FROM {{ mapping.clinical_task_has_document }} thd
    JOIN annotation_context ac ON ac.annotation_id = thd.task_id
    WHERE thd.type = 'input'
),
ssnv AS (
    -- Selected on the document's type fields, never its filename. Counted over one task, so two
    -- matches can only be a mistyped document.
    SELECT thd.task_id           AS task_id,
           MAX(d.url)            AS url,
           COUNT(DISTINCT d.url) AS matches
    FROM task_size ts
    JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = ts.task_id
                                                     AND thd.type = 'output'
    JOIN {{ mapping.clinical_document }} d            ON d.id = thd.document_id
                                                     AND d.data_type_code = 'ssnv'
                                                     AND d.format_code    = 'vcf'
    GROUP BY thd.task_id
),
annotated AS (
    -- The (case, calling task) pairs whose `ssnv` VCF is the input of an annotation on that case.
    SELECT DISTINCT ac.case_id AS case_id,
                    thd.task_id AS task_id
    FROM annotation_input ai
    JOIN annotation_context ac                         ON ac.annotation_id = ai.annotation_id
    JOIN {{ mapping.clinical_task_has_document }} thd  ON thd.document_id = ai.document_id
                                                      AND thd.type = 'output'
    JOIN {{ mapping.clinical_document }} d             ON d.id = thd.document_id
                                                      AND d.data_type_code = 'ssnv'
                                                      AND d.format_code    = 'vcf'
),
candidate AS (
    SELECT ct.case_id AS case_id,
           ct.task_id AS task_id
    FROM case_task ct
    LEFT JOIN ssnv s              ON s.task_id = ct.task_id
    LEFT JOIN annotated a         ON a.case_id = ct.case_id
                                 AND a.task_id = ct.task_id
    WHERE a.task_id IS NULL
    {% if params.task_ids %}
      -- Targeted rerun: the named tasks only, including one with no `ssnv` VCF, so that the
      -- operator sees why it cannot run instead of silence.
      AND ct.task_id IN %(task_ids)s
    {% else %}
      -- A task with no `ssnv` VCF is not a candidate on a discovery run: a calling task sent the
      -- old way, with its annotation from QLIN, would otherwise be reported every night.
      AND s.task_id IS NOT NULL
    {% endif %}
)
SELECT sc.case_id                     AS case_id,
       sc.submitter_case_id           AS submitter_case_id,
       sc.tenant_code                 AS tenant_code,
       pr.code                        AS project_code,
       te.task_id                     AS calling_task_id,
       te.task_type                   AS calling_task_type,
       se.id                          AS seq_id,
       se.aliquot                     AS aliquot,
       se.experimental_strategy_code  AS strategy,
       sa.histology_code              AS histology,
       s.url                          AS ssnv_url,
       COALESCE(s.matches, 0)         AS ssnv_matches,
       -- Task-level reasons first: they hold for every case of the task. `pending_sequencing` is
       -- transient, the task waits for its experiments rather than running on unfinished data.
       CASE
           WHEN se.status_code <> 'completed' THEN 'pending_sequencing'
           WHEN s.task_id IS NULL             THEN 'no_ssnv'
           WHEN s.matches > 1                 THEN 'ambiguous_ssnv'
           WHEN pr.code IS NULL               THEN 'no_project_code'
           {% if params.excluded_tenants %}
           WHEN sc.tenant_code IN %(excluded_tenants)s THEN 'tenant_excluded'
           {% endif %}
           ELSE NULL
       END                            AS exclusion_reason
FROM candidate cd
JOIN somatic_case sc                                  ON sc.case_id = cd.case_id
JOIN task_experiment te                               ON te.task_id = cd.task_id
JOIN {{ mapping.clinical_sequencing_experiment }} se  ON se.id = te.seq_id
LEFT JOIN {{ mapping.clinical_sample }} sa            ON sa.id = se.sample_id
LEFT JOIN ssnv s                                      ON s.task_id = cd.task_id
-- On the primary key alone, as in the germline queries: the batch PATCH looks a case up by
-- (project_code, submitter_case_id).
LEFT JOIN {{ mapping.clinical_project }} pr           ON pr.id = sc.project_id
ORDER BY te.task_id,
         sc.case_id,
         CASE sa.histology_code WHEN 'tumoral' THEN 0 ELSE 1 END,
         se.id
