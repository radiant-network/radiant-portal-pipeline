CREATE VIEW IF NOT EXISTS sequencing_experiment_delta
AS
SELECT * FROM iceberg.poc_starrocks.kf_sequencing_experiment kf
LEFT ANTI JOIN sequencing_experiment se
ON kf.seq_id = se.seq_id
AND kf.part = se.part
AND kf.sample_id = se.sample_id
