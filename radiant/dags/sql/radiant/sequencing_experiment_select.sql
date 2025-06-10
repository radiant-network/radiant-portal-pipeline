SELECT * FROM {{ params.starrocks_staging_sequencing_experiment }} se
WHERE se.updated_at >= COALESCE(se.ingested_at, '1970-01-01 00:00:00')
