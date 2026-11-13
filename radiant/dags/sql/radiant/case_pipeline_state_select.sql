-- The last state of every unit, compared with the decisions of a pass: only the ones that changed are written.
SELECT pipeline, case_id, unit_id, scope, patient_id, seq_id, task_id, decision, reason
FROM {{ mapping.starrocks_case_pipeline_state }}
