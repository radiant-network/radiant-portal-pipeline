-- RAD-272 — the somatic CNV VCF moves from `tumor_only_variant_calling` to `alignment_somatic_variant_calling`.
--
-- The somatic QLIN contract of 2026-11-10 sends one task per Dragen run: the tumor-only run is one
-- `alignment_somatic_variant_calling` task, which carries the somatic CNV VCF (`scnv`). QLIN no longer sends
-- `tumor_only_variant_calling`. staging_external_sequencing_experiment now reads `scnv` on the new type only, and the
-- import no longer knows the old one.
--
-- A `tumor_only_variant_calling` row left in staging_sequencing_experiment would break the import: the parts are built
-- from the staging rows before anything else, and a row of an unknown type fails the whole part (`Unknown task type`).
-- This happens to any such row the import selects: one never ingested, or one marked deleted when its task is removed
-- from the clinical model. And the swap that purges the somatic CNVs of a deleted task only runs for the new type: a
-- row deleted without this migration would leave its occurrences behind for good. So the rows and their occurrences
-- are removed here, before the first import.
--
-- WHERE. Substitute the placeholders before a run:
--   <shared_db>          the base database, that holds staging_sequencing_experiment
--   <tenant>_tenant      each tenant database that holds somatic__cnv__occurrence (section 2, once per tenant)
--   <task_ids>           the task_id list of check 0b (section 2)
--
-- WHEN. After the ETL deploy, with no radiant-import / import_part running, before the next radiant-import.


-- ---------------------------------------------------------------------------------------------------
-- 0. Read-only. Empty 0a means sections 2 and 3 have nothing to do.
-- ---------------------------------------------------------------------------------------------------

-- 0a. The `tumor_only_variant_calling` rows of the staging table.
SELECT tenant_code, deleted, ingested_at IS NULL AS never_ingested,
       COUNT(*) AS n_rows, COUNT(DISTINCT task_id) AS n_tasks, COUNT(DISTINCT part) AS n_parts
FROM <shared_db>.staging_sequencing_experiment
WHERE task_type = 'tumor_only_variant_calling'
GROUP BY tenant_code, deleted, ingested_at IS NULL;

-- 0b. Their task ids, for section 2.
SELECT GROUP_CONCAT(DISTINCT CAST(task_id AS VARCHAR)) AS task_ids
FROM <shared_db>.staging_sequencing_experiment
WHERE task_type = 'tumor_only_variant_calling';

-- 0c. Per tenant: the somatic CNV occurrences of those tasks.
SELECT COUNT(*) AS n_occurrences, COUNT(DISTINCT task_id) AS n_tasks, COUNT(DISTINCT part) AS n_parts
FROM <tenant>_tenant.somatic__cnv__occurrence
WHERE task_id IN (<task_ids>);


-- ---------------------------------------------------------------------------------------------------
-- 1. The fixed views. `CREATE VIEW IF NOT EXISTS` keeps the old definition: drop both, then run the Init StarRocks
--    Tables DAG, which re-creates them.
-- ---------------------------------------------------------------------------------------------------

DROP VIEW IF EXISTS <shared_db>.staging_sequencing_experiment_delta;
DROP VIEW IF EXISTS <shared_db>.staging_external_sequencing_experiment;

--
--   SHOW CREATE VIEW <shared_db>.staging_external_sequencing_experiment;
--       -- the `scnv` branch of the WHERE reads `t.task_type_code = 'alignment_somatic_variant_calling'`
--


-- ---------------------------------------------------------------------------------------------------
-- 2. Per tenant, when 0c is not 0: delete the occurrences of those tasks. somatic__cnv__occurrence is a Duplicate Key
--    table, which takes no subquery in a DELETE: paste the list of 0b. The Iceberg table needs nothing, the next
--    import overwrites it.
-- ---------------------------------------------------------------------------------------------------

DELETE FROM <tenant>_tenant.somatic__cnv__occurrence
WHERE task_id IN (<task_ids>);

--
--   Re-run 0c: it returns 0. somatic__cnv__variant_frequency still counts the deleted occurrences until the next
--   import of an `alignment_somatic_variant_calling` task, which recomputes it in full.
--


-- ---------------------------------------------------------------------------------------------------
-- 3. Delete the rows of 0a, after section 2: 0b reads them.
-- ---------------------------------------------------------------------------------------------------

DELETE FROM <shared_db>.staging_sequencing_experiment
WHERE task_type = 'tumor_only_variant_calling';

--
--   Re-run 0a: it returns nothing.
--
