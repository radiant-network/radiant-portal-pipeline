INSERT INTO {{ mapping.starrocks_pipeline_decision_log }} (
    run_id, stage, pipeline, case_id, scope, tenant_code, patient_id, seq_id, task_id, unit_id, decision, reason, detail,
    decided_at
) VALUES (
    %(run_id)s, %(stage)s, %(pipeline)s, %(case_id)s, %(scope)s, %(tenant_code)s, %(patient_id)s, %(seq_id)s,
    %(task_id)s, %(unit_id)s, %(decision)s, %(reason)s, %(detail)s, %(decided_at)s
)
