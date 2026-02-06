UPDATE {{ mapping.starrocks_staging_sequencing_experiment }}
SET deleted = TRUE
WHERE task_id NOT IN (
    SELECT id FROM {{ mapping.clinical_task }}
)