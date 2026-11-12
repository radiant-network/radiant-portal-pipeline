-- The cases the import still has work for, and why.

-- Changed in the clinical model, not in staging yet.
SELECT case_id, tenant_code, 'new_or_changed' AS reason
FROM {{ mapping.starrocks_staging_sequencing_experiment_delta }}
UNION
-- In staging, not loaded by import_part yet, or deleted.
SELECT se.case_id, se.tenant_code, IF(se.deleted, 'deleted', 'not_loaded') AS reason
FROM {{ mapping.starrocks_staging_sequencing_experiment }} se
WHERE se.updated_at >= COALESCE(se.ingested_at, '1970-01-01 00:00:00') OR se.deleted
