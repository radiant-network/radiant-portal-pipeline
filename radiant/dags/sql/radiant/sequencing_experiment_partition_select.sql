SELECT
    case_id,
    seq_id,
    task_id,
    part,
    analysis_type,
    sample_id,
    patient_id,
    experimental_strategy,
    request_id,
    request_priority,
    vcf_filepath,
    sex,
    family_role,
    affected_status,
    created_at,
    updated_at,
    ingested_at
FROM {{ params.starrocks_sequencing_experiment }}
WHERE
    part=%(part)s and
    updated_at >= COALESCE(ingested_at, '1970-01-01 00:00:00')
ORDER BY
    case_id, seq_id, task_id