UPDATE {{ mapping.starrocks_open_data_release }}
SET reannotated_snapshot_id = imported_snapshot_id,
    recorded_at = NOW(),
    dag_run_id  = '{{ run_id }}'
WHERE source_name IS NOT NULL;
