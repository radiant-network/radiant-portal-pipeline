-- One row per (germline case, family member): the member's current sequencing experiment, the
-- germline CNV VCF (and CRAM, when there is one) of that experiment's current alignment, and
-- whether the case already has a CNV annotation for it.
--
-- Facts only: every germline case of `pipeline_case` comes back, and the decision is made in
-- Python by `radiant.tasks.nextflow.germline_cnv.resolve.select_cases`. Read by the case
-- scheduler (every case), and by `radiant-nextflow-cnv-postprocessing-cases` (`case_ids`: the
-- cases the scheduler decided to run, with every member).
--
-- Sibling of `germline_snv_facts_select.sql`: both read the supersession policy from the views
-- `current_experiment` and `current_alignment`. What differs is the trigger document (a `gcnv`
-- VCF here, a gVCF there) and the annotation task. The CRAM is carried, not required: Python
-- decides per family whether every member has one.
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
)
{% if live_only is defined and live_only %},
-- The scheduler's discovery reads the live cases only: work not done yet, a status still moving, or a last
-- state that is not settled (`done`, `no_trigger_document`). A settled case keeps its state in
-- `case_pipeline_state` until its facts give it work again.
settled_case AS (
    SELECT case_id
    FROM {{ mapping.starrocks_case_pipeline_state }}
    WHERE pipeline = 'germline_cnv'
      AND scope = 'case'
    GROUP BY case_id
    HAVING SUM(CASE WHEN decision = 'done' OR reason = 'no_trigger_document' THEN 0 ELSE 1 END) = 0
),
live_case AS (
    SELECT ce.case_id
    FROM {{ mapping.starrocks_current_experiment }} ce
    JOIN gcnv g               ON g.seq_id = ce.seq_id
    LEFT JOIN cnv_annotated a ON a.case_id = ce.case_id
                             AND a.seq_id = ce.seq_id
    WHERE a.seq_id IS NULL
    UNION
    SELECT pc.case_id
    FROM {{ mapping.starrocks_pipeline_case }} pc
    LEFT JOIN settled_case s ON s.case_id = pc.case_id
    WHERE pc.status_code IN ('submitted', 'processing')
       OR s.case_id IS NULL
)
{% endif %}
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
       ca.task_id                      AS alignment_task_id,
       -- The caller, so a non-DRAGEN alignment is excluded here rather than failing hours
       -- in: the pipeline's normalization step only implements DRAGEN's CNV conventions.
       ca.pipeline_name                AS alignment_pipeline,
       g.url                           AS gcnv_url,
       COALESCE(g.matches, 0)          AS gcnv_matches,
       cr.url                          AS cram_url,
       ci.url                          AS crai_url,
       a.seq_id IS NOT NULL            AS annotated
FROM {{ mapping.starrocks_pipeline_case }} pc
JOIN {{ mapping.starrocks_case_member }} cm          ON cm.case_id = pc.case_id
-- LEFT, all of them: a member with no experiment, alignment or CNV VCF comes back, so Python can
-- say why. A family silently short one member is the worst failure available.
LEFT JOIN {{ mapping.starrocks_current_experiment }} ce ON ce.case_id = pc.case_id
                                                    AND ce.patient_id = cm.patient_id
LEFT JOIN current_alignment ca                       ON ca.seq_id = ce.seq_id
LEFT JOIN gcnv g                                     ON g.seq_id = ce.seq_id
LEFT JOIN cram cr                                    ON cr.seq_id = ce.seq_id
LEFT JOIN crai ci                                    ON ci.seq_id = ce.seq_id
LEFT JOIN cnv_annotated a                            ON a.case_id = pc.case_id
                                                    AND a.seq_id = ce.seq_id
WHERE pc.case_type = 'germline'
{% if live_only is defined and live_only %}
  AND pc.case_id IN (SELECT case_id FROM live_case)
{% endif %}
{% if case_ids is defined and case_ids %}
  AND pc.case_id IN %(case_ids)s
{% endif %}
ORDER BY pc.case_id,
         CASE cm.role
              WHEN 'proband' THEN 0
              WHEN 'father'  THEN 1
              WHEN 'mother'  THEN 2
              ELSE 3
         END,
         cm.patient_id
