CREATE TABLE IF NOT EXISTS {{ params.starrocks_sequencing_experiments }} (
    seq_id INT,
    part INT,
    sample_id VARCHAR(255),
    created_at DATETIME
)
PRIMARY KEY (seq_id, part, sample_id)
