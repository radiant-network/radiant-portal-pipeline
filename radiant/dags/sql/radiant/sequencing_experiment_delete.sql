DELETE FROM {{ mapping.starrocks_staging_sequencing_experiment }}
WHERE task_id in %(deleted_task_ids)s