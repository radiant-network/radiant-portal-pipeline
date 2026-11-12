-- The cases the scheduled import still has work for, and why: one row per case and reason. A case with several
-- experiments or tasks can come back with several reasons.
--
-- Consumed by `radiant-case-scheduler` (`discover_import_cases`, `rediscover_import_cases`), which logs one `run`
-- decision per case, with the first reason in the order new_or_changed, not_loaded, deleted.

-- The delta: what changed in the clinical model and is not in staging yet. A task registered on a case
-- (e.g. a new annotation) shows up here until the import inserts it into `staging_sequencing_experiment`.
SELECT case_id, tenant_code, 'new_or_changed' AS reason
FROM {{ mapping.starrocks_staging_sequencing_experiment_delta }}
UNION
-- The staging rows `import_part` has not loaded yet, with the import's own predicate
-- (`sequencing_experiment_select.sql`). A row is inserted with `ingested_at` at the epoch: it stays here, as
-- `not_loaded`, until `import_part` loads it, so a failed `import_part` is picked up by a later import. A task
-- deleted in the portal is flagged `deleted` and stays here until the import removes its data; it also matches
-- the date predicate, hence the IF.
SELECT se.case_id, se.tenant_code, IF(se.deleted, 'deleted', 'not_loaded') AS reason
FROM {{ mapping.starrocks_staging_sequencing_experiment }} se
WHERE se.updated_at >= COALESCE(se.ingested_at, '1970-01-01 00:00:00') OR se.deleted
