CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_staging_sequencing_experiment }} (
    seq_id INT NOT NULL,
    task_id INT NOT NULL,
    task_type VARCHAR(100) NOT NULL,
    part INT NOT NULL,
    analysis_type VARCHAR(50),
    aliquot VARCHAR(255),
    patient_id VARCHAR(255),
    experimental_strategy VARCHAR(50),
    request_priority VARCHAR(20),
    vcf_filepath VARCHAR(1024),
    cnv_vcf_filepath VARCHAR(1024),
    exomiser_filepath VARCHAR(1024),
    sex VARCHAR(10),
    family_role VARCHAR(20),
    affected_status VARCHAR(20),
    created_at DATETIME,
    updated_at DATETIME,
    ingested_at DATETIME,
    deleted BOOLEAN NOT NULL DEFAULT "false"
)
PRIMARY KEY (seq_id, task_id)
