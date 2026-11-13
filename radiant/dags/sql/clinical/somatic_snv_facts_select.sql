-- One row per (somatic calling task, somatic case it belongs to, sequencing experiment it called):
-- the raw somatic SNV VCF the task published, the experiment's status, and whether an annotation
-- of that VCF is already registered on the case. Plus one row, with no calling task, per somatic
-- case that no calling task belongs to.
--
-- Facts only: every somatic case of `pipeline_case` comes back, and the decision is made in Python
-- by `radiant.tasks.nextflow.somatic_snv.resolve.select_tasks`. Read by the case scheduler (every
-- case), and by `radiant-nextflow-somatic-snv-annotation-cases` (`case_ids`: the cases the
-- scheduler decided to run). Design: radiant-hub `docs/features/RAD-23-somatic-vep-annotation.md`.
--
-- The unit of work is the (calling task, case) pair: a tumor-normal and a tumor-only call on the
-- same tumor sample are two VCFs, and both are annotated. Annotated per case, not globally: a
-- registration that succeeded in one tenant must not hide a case of another. Only the `ssnv` VCF
-- is a trigger; the `scnv` VCF of the same task never is.
WITH work_unit_experiment AS (
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
registration_case AS (
    -- The somatic cases of `pipeline_case` (RAD-287: not draft or revoked) that hold *every*
    -- experiment of a calling task: one row per (case, calling task), with what the batch PATCH
    -- needs to find the case.
    --
    -- Through `case_has_sequencing_experiment`, never `task_context.case_id`: only
    -- `tumor_only_variant_calling` carries one. Every experiment, because the portal refuses an
    -- input document produced by an experiment the case does not hold.
    SELECT pc.case_id           AS case_id,
           pc.submitter_case_id AS submitter_case_id,
           pc.tenant_code       AS tenant_code,
           pc.project_code      AS project_code,
           te.task_id           AS task_id
    FROM work_unit_experiment te
    JOIN {{ mapping.clinical_case_has_sequencing_experiment }} chse ON chse.sequencing_experiment_id = te.seq_id
    JOIN {{ mapping.starrocks_pipeline_case }} pc              ON pc.case_id = chse.case_id
                                                              AND pc.case_type = 'somatic'
    GROUP BY pc.case_id, pc.submitter_case_id, pc.tenant_code, pc.project_code, te.task_id, te.task_experiments
    HAVING COUNT(DISTINCT te.seq_id) = te.task_experiments
),
vcf_to_annotate AS (
    -- The raw somatic SNV VCF each calling task published: what the pipeline annotates, and the
    -- input the annotation is registered with. Selected on the document's type fields, never its
    -- filename, so the `scnv` VCF and the index are never picked. Counted over one task, so two
    -- matches can only be a mistyped document.
    SELECT thd.task_id           AS task_id,
           MAX(d.url)            AS url,
           COUNT(DISTINCT d.url) AS matches
    FROM work_unit_experiment te
    JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = te.task_id
                                                     AND thd.type = 'output'
    JOIN {{ mapping.clinical_document }} d            ON d.id = thd.document_id
                                                     AND d.data_type_code = 'ssnv'
                                                     AND d.format_code    = 'vcf'
    GROUP BY thd.task_id
),
already_registered AS (
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
)
{% if live_only is defined and live_only %},
-- The scheduler's discovery reads the live cases only: work not done yet, a status still moving, or a last
-- state that is not settled (`done`, `no_trigger_document`). A settled case keeps its state in
-- `case_pipeline_state` until its facts give it work again.
settled_case AS (
    SELECT case_id
    FROM {{ mapping.starrocks_case_pipeline_state }}
    WHERE pipeline = 'somatic_snv'
      AND scope = 'case'
    GROUP BY case_id
    HAVING SUM(CASE WHEN decision = 'done' OR reason = 'no_trigger_document' THEN 0 ELSE 1 END) = 0
),
live_case AS (
    SELECT rc.case_id
    FROM registration_case rc
    JOIN vcf_to_annotate v          ON v.task_id = rc.task_id
    LEFT JOIN already_registered a  ON a.case_id = rc.case_id
                                   AND a.task_id = rc.task_id
    WHERE a.task_id IS NULL
    UNION
    SELECT pc.case_id
    FROM {{ mapping.starrocks_pipeline_case }} pc
    LEFT JOIN settled_case s ON s.case_id = pc.case_id
    WHERE pc.status_code IN ('submitted', 'processing')
       OR s.case_id IS NULL
)
{% endif %}
SELECT rc.case_id                     AS case_id,
       rc.submitter_case_id           AS submitter_case_id,
       rc.tenant_code                 AS tenant_code,
       rc.project_code                AS project_code,
       te.task_id                     AS calling_task_id,
       te.task_type                   AS calling_task_type,
       se.id                          AS seq_id,
       se.status_code                 AS seq_status,
       se.aliquot                     AS aliquot,
       se.experimental_strategy_code  AS strategy,
       sa.histology_code              AS histology,
       v.url                          AS ssnv_url,
       COALESCE(v.matches, 0)         AS ssnv_matches,
       a.task_id IS NOT NULL          AS annotated
FROM registration_case rc
JOIN work_unit_experiment te                          ON te.task_id = rc.task_id
JOIN {{ mapping.clinical_sequencing_experiment }} se  ON se.id = te.seq_id
LEFT JOIN {{ mapping.clinical_sample }} sa            ON sa.id = se.sample_id
LEFT JOIN vcf_to_annotate v                           ON v.task_id = rc.task_id
LEFT JOIN already_registered a                        ON a.case_id = rc.case_id
                                                     AND a.task_id = rc.task_id
WHERE 1 = 1
{% if case_ids is defined and case_ids %}
  AND rc.case_id IN %(case_ids)s
{% endif %}
{% if live_only is defined and live_only %}
  AND rc.case_id IN (SELECT case_id FROM live_case)
{% endif %}
UNION ALL
-- The somatic cases no calling task belongs to: nothing to annotate.
SELECT pc.case_id, pc.submitter_case_id, pc.tenant_code, pc.project_code,
       CAST(NULL AS BIGINT), CAST(NULL AS VARCHAR), CAST(NULL AS BIGINT), CAST(NULL AS VARCHAR),
       CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR),
       0, FALSE
FROM {{ mapping.starrocks_pipeline_case }} pc
LEFT JOIN (SELECT DISTINCT case_id FROM registration_case) rcs ON rcs.case_id = pc.case_id
WHERE pc.case_type = 'somatic'
  AND rcs.case_id IS NULL
{% if live_only is defined and live_only %}
  AND pc.case_id IN (SELECT case_id FROM live_case)
{% endif %}
{% if case_ids is defined and case_ids %}
  AND pc.case_id IN %(case_ids)s
{% endif %}
ORDER BY calling_task_id, case_id, histology DESC, seq_id
