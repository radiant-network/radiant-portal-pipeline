SELECT * FROM (
	SELECT * FROM {{ params.starrocks_sequencing_experiment }} se
	WHERE se.updated_at >= COALESCE(se.ingested_at, '1970-01-01 00:00:00')
	UNION ALL
	SELECT sed.*, '1970-01-01 00:00:00' as ingested_at
	FROM {{ params.starrocks_sequencing_experiment_delta }} sed
) se WHERE se.part=%(part)s