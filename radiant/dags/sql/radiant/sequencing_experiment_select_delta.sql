SELECT
    case_id,
    seq_id,
    part,
    analysis_type,
    sample_id,
    patient_id,
    vcf_filepath,
    sex,
    family_role,
    is_affected,
    created_at,
    updated_at,
    ingested_at
FROM {{ params.source_sequencing_experiments }}
WHERE
    part=%(part)s and
    updated_at >= COALESCE(ingested_at, '1970-01-01 00:00:00')
