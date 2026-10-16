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
WITH calling_task_experiment AS (
    -- Every somatic calling task, one row per sequencing experiment it called, with the number of
    -- experiments of the task. These three types are the ones QLIN sends with the raw VCF; listing
    -- them also keeps the DAG's own `radiant_somatic_annotation` (an `ssnv` VCF too) out.
    --
    -- DISTINCT: `tumor_only_variant_calling` is case-related in the portal, so it can have one
    -- context row per case for the same experiment. Rows are then unique per (task, experiment),
    -- which makes `COUNT(*)` over the task its number of experiments.
    SELECT task_id,
           task_type,
           seq_id,
           COUNT(*) OVER (PARTITION BY task_id) AS task_experiments
    FROM (
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
    ) task_experiment
),
case_calling_task AS (
    -- The somatic cases, not draft or revoked, that hold *every* experiment of a calling task: one
    -- row per (case, calling task), with what the batch PATCH needs to find the case.
    --
    -- Through `case_has_sequencing_experiment`, never `task_context.case_id`: only
    -- `tumor_only_variant_calling` carries one. Every experiment, because the portal refuses an
    -- input document produced by an experiment the case does not hold.
    SELECT c.id                AS case_id,
           c.submitter_case_id AS submitter_case_id,
           c.tenant_code       AS tenant_code,
           c.project_id        AS project_id,
           te.task_id          AS task_id
    FROM calling_task_experiment te
    JOIN {{ mapping.clinical_case_has_sequencing_experiment }} chse ON chse.sequencing_experiment_id = te.seq_id
    JOIN {{ mapping.clinical_case }} c                         ON c.id = chse.case_id
                                                              AND c.case_type_code = 'somatic'
                                                              AND c.status_code NOT IN ({{ mapping.pipeline_ignored_case_statuses }})
    GROUP BY c.id, c.submitter_case_id, c.tenant_code, c.project_id, te.task_id, te.task_experiments
    HAVING COUNT(DISTINCT te.seq_id) = te.task_experiments
),
raw_somatic_vcf AS (
    -- The raw somatic SNV VCF each calling task published: what the pipeline annotates, and the
    -- input the annotation is registered with. Selected on the document's type fields, never its
    -- filename, so the `scnv` VCF and the index are never picked. Counted over one task, so two
    -- matches can only be a mistyped document.
    SELECT thd.task_id           AS task_id,
           MAX(d.url)            AS url,
           COUNT(DISTINCT d.url) AS matches
    FROM calling_task_experiment te
    JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = te.task_id
                                                     AND thd.type = 'output'
    JOIN {{ mapping.clinical_document }} d            ON d.id = thd.document_id
                                                     AND d.data_type_code = 'ssnv'
                                                     AND d.format_code    = 'vcf'
    GROUP BY thd.task_id
),
already_annotated AS (
    -- The (case, calling task) pairs an earlier run already annotated: an output of the calling
    -- task is the input of a `radiant_somatic_annotation` registered on that case. The query keeps
    -- no other record of what it did. The annotation type is case-related in the portal, so its
    -- context carries the case.
    SELECT DISTINCT tc.case_id       AS case_id,
                    call_out.task_id AS task_id
    FROM {{ mapping.clinical_task }} t
    JOIN {{ mapping.clinical_task_context }} tc            ON tc.task_id = t.id
                                                         AND tc.case_id IS NOT NULL
    JOIN {{ mapping.clinical_task_has_document }} ann_in   ON ann_in.task_id = t.id
                                                         AND ann_in.type = 'input'
    JOIN {{ mapping.clinical_task_has_document }} call_out ON call_out.document_id = ann_in.document_id
                                                         AND call_out.type = 'output'
    WHERE t.task_type_code = 'radiant_somatic_annotation'
),
pending_case_task AS (
    -- The (case, calling task) pairs left to run: `case_calling_task` minus `already_annotated`
    -- (the LEFT JOIN ... IS NULL anti-join).
    SELECT ct.case_id           AS case_id,
           ct.submitter_case_id AS submitter_case_id,
           ct.tenant_code       AS tenant_code,
           ct.project_id        AS project_id,
           ct.task_id           AS task_id
    FROM case_calling_task ct
    LEFT JOIN raw_somatic_vcf v   ON v.task_id = ct.task_id
    LEFT JOIN already_annotated a ON a.case_id = ct.case_id
                                 AND a.task_id = ct.task_id
    WHERE a.task_id IS NULL
    {% if params.task_ids %}
      -- Targeted rerun: the named tasks only, including one with no `ssnv` VCF, so that the
      -- operator sees why it cannot run instead of silence.
      AND ct.task_id IN %(task_ids)s
    {% else %}
      -- A task with no `ssnv` VCF is not a candidate on a discovery run: a calling task sent the
      -- old way, with its annotation from QLIN, would otherwise be reported every night.
      AND v.task_id IS NOT NULL
    {% endif %}
)
SELECT pt.case_id                     AS case_id,
       pt.submitter_case_id           AS submitter_case_id,
       pt.tenant_code                 AS tenant_code,
       pr.code                        AS project_code,
       te.task_id                     AS calling_task_id,
       te.task_type                   AS calling_task_type,
       se.id                          AS seq_id,
       se.aliquot                     AS aliquot,
       se.experimental_strategy_code  AS strategy,
       sa.histology_code              AS histology,
       v.url                          AS ssnv_url,
       COALESCE(v.matches, 0)         AS ssnv_matches,
       -- Task-level reasons first: they hold for every case of the task. `pending_sequencing` is
       -- transient, the task waits for its experiments rather than running on unfinished data.
       CASE
           WHEN se.status_code <> 'completed' THEN 'pending_sequencing'
           WHEN v.task_id IS NULL             THEN 'no_ssnv'
           WHEN v.matches > 1                 THEN 'ambiguous_ssnv'
           WHEN pr.code IS NULL               THEN 'no_project_code'
           {% if params.excluded_tenants %}
           WHEN pt.tenant_code IN %(excluded_tenants)s THEN 'tenant_excluded'
           {% endif %}
           ELSE NULL
       END                            AS exclusion_reason
FROM pending_case_task pt
JOIN calling_task_experiment te                       ON te.task_id = pt.task_id
JOIN {{ mapping.clinical_sequencing_experiment }} se  ON se.id = te.seq_id
LEFT JOIN {{ mapping.clinical_sample }} sa            ON sa.id = se.sample_id
LEFT JOIN raw_somatic_vcf v                           ON v.task_id = pt.task_id
-- On the primary key alone, as in the germline queries: the batch PATCH looks a case up by
-- (project_code, submitter_case_id).
LEFT JOIN {{ mapping.clinical_project }} pr           ON pr.id = pt.project_id
ORDER BY te.task_id,
         pt.case_id,
         CASE sa.histology_code WHEN 'tumoral' THEN 0 ELSE 1 END,
         se.id
