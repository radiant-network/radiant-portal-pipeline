CREATE TABLE IF NOT EXISTS {{ params.starrocks_sequencing_experiment }} (
    case_id INT NOT NULL,
    seq_id INT NOT NULL,
    task_id INT NOT NULL,
    part INT NOT NULL,
    analysis_type VARCHAR(50),
    sample_id VARCHAR(255),
    patient_id VARCHAR(255),
    experimental_strategy VARCHAR(50),
    request_id INT,
    request_priority VARCHAR(20),
    vcf_filepath VARCHAR(1024),
    sex VARCHAR(10),
    family_role VARCHAR(20),
    affected_status VARCHAR(20),
    created_at DATETIME,
    updated_at DATETIME,
    ingested_at DATETIME
)
PRIMARY KEY (case_id, seq_id, task_id)
