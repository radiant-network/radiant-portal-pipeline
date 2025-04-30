CREATE VIEW IF NOT EXISTS {{ params.starrocks_sequencing_experiments_delta }}
AS
SELECT * FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.{{ params.iceberg_ }}.{{ params.iceberg_sequencing_experiments }} kf
LEFT ANTI JOIN {{ params.starrocks_sequencing_experiments_delta }} se
ON kf.seq_id = se.seq_id
AND kf.part = se.part
AND kf.sample_id = se.sample_id
