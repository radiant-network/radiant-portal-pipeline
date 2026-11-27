-- The staged rows of the tasks the import decided `task_deleted` for: `import_part` purges their data.
UPDATE {{ mapping.starrocks_staging_sequencing_experiment }}
SET deleted = TRUE
WHERE task_id IN %(task_ids)s
