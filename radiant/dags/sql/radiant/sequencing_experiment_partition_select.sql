SELECT
    seq_id,
    task_id,
    task_type,
    part,
    analysis_type,
    aliquot,
    patient_id,
    experimental_strategy,
    request_priority,
    vcf_filepath,
    exomiser_filepath,
    cnv_vcf_filepath,
    sex,
    family_role,
    affected_status,
    created_at,
    updated_at,
    ingested_at,
    deleted
FROM {{ mapping.starrocks_staging_sequencing_experiment }}
WHERE
    part=%(part)s and
    (updated_at >= COALESCE(ingested_at, '1970-01-01 00:00:00') or deleted)
ORDER BY
    seq_id, task_id