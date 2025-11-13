INSERT INTO {{ mapping.starrocks_staging_sequencing_experiment }} (
            case_id, seq_id, task_id, part, analysis_type, aliquot, patient_id, experimental_strategy,
            request_priority, vcf_filepath, cnv_vcf_filepath, exomiser_filepath, sex, family_role, affected_status,
            created_at, updated_at, ingested_at
) VALUES (
    %(case_id)s, %(seq_id)s, %(task_id)s, %(part)s, %(analysis_type)s, %(aliquot)s, %(patient_id)s,
    %(experimental_strategy)s, %(request_priority)s, %(vcf_filepath)s, %(cnv_vcf_filepath)s, %(exomiser_filepath)s, %(sex)s,
    %(family_role)s, %(affected_status)s, %(created_at)s, %(updated_at)s, %(ingested_at)s
)