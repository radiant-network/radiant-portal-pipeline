-- One row per member of the named germline cases (`case_ids`): what the post-processing pipeline runs on.
-- The case scheduler decided these cases on `germline_snv_facts_select.sql`; both read the gVCF from the view
-- `current_germline_gvcf`. A member with no gVCF yet comes back with a null `gvcf_url`.
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
       COALESCE(g.matches, 0)          AS gvcf_matches
FROM {{ mapping.starrocks_pipeline_case }} pc
JOIN {{ mapping.starrocks_case_member }} cm              ON cm.case_id = pc.case_id
LEFT JOIN {{ mapping.starrocks_current_experiment }} ce  ON ce.case_id = pc.case_id
                                                        AND ce.patient_id = cm.patient_id
LEFT JOIN {{ mapping.starrocks_current_germline_gvcf }} g ON g.seq_id = ce.seq_id
WHERE pc.case_type = 'germline'
  AND pc.case_id IN %(case_ids)s
ORDER BY pc.case_id, cm.patient_id
