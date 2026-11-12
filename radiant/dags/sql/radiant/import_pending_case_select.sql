-- The cases the scheduled import still has work for, with why: one row per case and reason.
--
-- Consumed by `radiant-case-scheduler` (`discover_import_cases`, `rediscover_import_cases`), which logs one
-- `run` decision per case. Two places, because a row leaves the first as soon as the import inserts
-- it into the second:
--
-- - the delta view (`new_or_changed`): sequencing experiments not yet in `staging_sequencing_experiment`, or
--   changed since;
-- - `staging_sequencing_experiment` rows `import_part` has not loaded yet (`not_loaded`), or flagged
--   `deleted`, with the import's own predicate (`sequencing_experiment_select.sql`). A row is inserted with
--   `ingested_at` at the epoch, so one whose `import_part` failed stays here until a later import loads it.
SELECT case_id, tenant_code, 'new_or_changed' AS reason
FROM {{ mapping.starrocks_staging_sequencing_experiment_delta }}
UNION
SELECT se.case_id, se.tenant_code, IF(se.deleted, 'deleted', 'not_loaded') AS reason
FROM {{ mapping.starrocks_staging_sequencing_experiment }} se
WHERE se.updated_at >= COALESCE(se.ingested_at, '1970-01-01 00:00:00') OR se.deleted
