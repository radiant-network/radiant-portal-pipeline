INSERT INTO {{ params.starrocks_sequencing_experiment }}
SELECT
    kf.seq_id,
    kf.part,
    kf.sample_id,
    current_timestamp() as created_at
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.{{ params.iceberg_ }}.{{ params.iceberg_sequencing_experiment }} kf
LEFT ANTI JOIN {{ params.starrocks_sequencing_experiment }} se
ON kf.seq_id = se.seq_id
AND kf.part = se.part
AND kf.sample_id = se.sample_id