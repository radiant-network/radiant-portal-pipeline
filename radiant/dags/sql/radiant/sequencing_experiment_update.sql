UPDATE {{ mapping.starrocks_staging_sequencing_experiment }}
SET ingested_at = NOW()
WHERE task_id in %(task_ids)s