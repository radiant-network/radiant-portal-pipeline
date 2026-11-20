-- One row per (candidate germline case, family member): the facts the case scheduler decides on, with no
-- decision in SQL. `decide_cases` (radiant/tasks/nextflow/resolve.py) turns them into a decision and its reasons.
--
-- Same views and CTEs as `pending_annotation_select.sql`, which the "from Cases" DAG still runs.
WITH current_alignment AS (
    -- The current germline alignment of each experiment (view `current_alignment`, RAD-287).
    SELECT seq_id, task_id
    FROM {{ mapping.starrocks_current_alignment }}
    WHERE task_type = 'alignment_germline_variant_calling'
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
    -- No registered result for a member's current experiment, documents or not, or no experiment at all yet:
    -- a case without any gVCF is a candidate too, and its reasons say why it does not run.
    SELECT DISTINCT pc.case_id AS case_id
    FROM {{ mapping.starrocks_pipeline_case }} pc
    LEFT JOIN {{ mapping.starrocks_current_experiment }} ce ON ce.case_id = pc.case_id
    LEFT JOIN annotated a                                ON a.case_id = ce.case_id
                                                        AND a.seq_id  = ce.seq_id
    WHERE pc.case_type = 'germline'
      AND (ce.seq_id IS NULL OR a.seq_id IS NULL)
)
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
       g.task_id                       AS alignment_task_id,
       g.url                           AS gvcf_url,
       COALESCE(g.matches, 0)          AS gvcf_matches,
       ca.task_id                      AS current_alignment_task_id
FROM {{ mapping.starrocks_pipeline_case }} pc
JOIN candidate cd                                    ON cd.case_id = pc.case_id
JOIN {{ mapping.starrocks_case_member }} cm          ON cm.case_id = pc.case_id
-- LEFT, all three: a member with no experiment, alignment or gVCF must come back, so its reason is decided.
LEFT JOIN {{ mapping.starrocks_current_experiment }} ce ON ce.case_id = pc.case_id
                                                    AND ce.patient_id = cm.patient_id
LEFT JOIN current_alignment ca                       ON ca.seq_id = ce.seq_id
LEFT JOIN gvcf g                                     ON g.seq_id = ce.seq_id
WHERE pc.case_type = 'germline'
ORDER BY pc.case_id,
         CASE cm.role
              WHEN 'proband' THEN 0
              WHEN 'father'  THEN 1
              WHEN 'mother'  THEN 2
              ELSE 3
         END,
         cm.patient_id
