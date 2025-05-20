CREATE TABLE IF NOT EXISTS {{ params.starrocks_sequencing_experiments }} (
    case_id INT NOT NULL,
    seq_id INT NOT NULL,
    part INT NOT NULL,
    analysis_type VARCHAR(50),
    sample_id VARCHAR(255),
    patient_id VARCHAR(255),
    vcf_filepath VARCHAR(1024),
    sex VARCHAR(10),
    family_role VARCHAR(20),
    is_affected BOOLEAN,
    created_at DATETIME,
    updated_at DATETIME,
    ingested_at DATETIME
)
PRIMARY KEY (case_id, seq_id)
