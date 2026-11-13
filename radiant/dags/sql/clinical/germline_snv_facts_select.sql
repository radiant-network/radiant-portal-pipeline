-- One row per (germline case, family member): the member's current sequencing experiment, the gVCF
-- of that experiment's current alignment, and whether the case already has an annotation for it.
--
-- Facts only: every germline case of `pipeline_case` comes back, and the decision (run, wait,
-- done, excluded, skipped) is made in Python by `radiant.tasks.nextflow.resolve.select_cases`.
-- Read by the case scheduler (every case), and by `radiant-nextflow-snv-postprocessing-cases`
-- (`case_ids`: the cases the scheduler decided to run, with every member).
--
-- Which experiment stands for each member, and which alignment for each experiment, is the
-- supersession policy of the views `current_experiment` and `current_alignment`.
WITH current_alignment AS (
    SELECT seq_id, task_id
    FROM {{ mapping.starrocks_current_alignment }}
    WHERE task_type = 'alignment_germline_variant_calling'
),
gvcf AS (
    -- Counted over the current task only: two gVCFs there can only be a mistyped document.
    -- Selected on the document's type fields, never its filename.
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
    -- Scoped to the case: annotating a shared experiment on case 1 must not make case 2 look done.
    SELECT DISTINCT tc.case_id                    AS case_id,
                    tc.sequencing_experiment_id   AS seq_id
    FROM {{ mapping.clinical_task }} t
    JOIN {{ mapping.clinical_task_context }} tc ON tc.task_id = t.id
    WHERE t.task_type_code = 'radiant_germline_annotation'
      AND tc.case_id IS NOT NULL
)
{% if live_only is defined and live_only %},
-- The scheduler's discovery reads the live cases only: work not done yet, a status still moving, or a last
-- state that is not settled (`done`, `no_trigger_document`). A settled case keeps its state in
-- `case_pipeline_state` until its facts give it work again.
settled_case AS (
    SELECT case_id
    FROM {{ mapping.starrocks_case_pipeline_state }}
    WHERE pipeline = 'germline_snv'
      AND scope = 'case'
    GROUP BY case_id
    HAVING SUM(CASE WHEN decision = 'done' OR reason = 'no_trigger_document' THEN 0 ELSE 1 END) = 0
),
live_case AS (
    SELECT ce.case_id
    FROM {{ mapping.starrocks_current_experiment }} ce
    JOIN gvcf g          ON g.seq_id = ce.seq_id
    LEFT JOIN annotated a ON a.case_id = ce.case_id
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
       pc.primary_condition            AS primary_condition,
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
       g.url                           AS gvcf_url,
       COALESCE(g.matches, 0)          AS gvcf_matches,
       a.seq_id IS NOT NULL            AS annotated
FROM {{ mapping.starrocks_pipeline_case }} pc
JOIN {{ mapping.starrocks_case_member }} cm             ON cm.case_id = pc.case_id
-- LEFT, all four: a member with no experiment, alignment or gVCF comes back, so Python can say why.
LEFT JOIN {{ mapping.starrocks_current_experiment }} ce ON ce.case_id = pc.case_id
                                                       AND ce.patient_id = cm.patient_id
LEFT JOIN current_alignment ca                          ON ca.seq_id = ce.seq_id
LEFT JOIN gvcf g                                        ON g.seq_id = ce.seq_id
LEFT JOIN annotated a                                   ON a.case_id = pc.case_id
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
