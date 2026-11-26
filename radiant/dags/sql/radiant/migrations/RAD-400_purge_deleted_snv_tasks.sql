-- RAD-400 — purge the SNV occurrences that deleted tasks left behind.
--
-- Before RAD-400, import_part skipped the SNV swaps of a part whose only SNV annotation tasks were deleted, then
-- deleted their staging rows. Their occurrences stayed, and no run can find them again: an occurrence whose task_id
-- has no row in staging_sequencing_experiment is such an orphan. Their staging frequencies stayed too, the tenant
-- frequencies and snv__variant still count them, and the partitioned tables still hold their part's rows.
--
-- WHERE. Substitute the placeholders before a run:
--   <shared_db>              the base database, that holds staging_sequencing_experiment, the staging frequencies
--                            and snv__consequence_filter_partitioned
--   <tenant>_tenant          each tenant database (sections 0a, 0b, 1, 0c and 3, once per tenant)
--   <tenant>                 its tenant_code (section 2)
--   <germline_task_ids>      the task_ids of 0a, <somatic_task_ids> those of 0b (section 1)
--   <parts>                  the parts of 0a and 0b of that tenant (sections 2 and 0c)
--   <variant_parts>          those parts divided by 10, rounded down (section 3)
--   <empty_parts>            the parts of 0a and 0b, all tenants, that 0c returns for no tenant (section 4)
--
-- WHEN. After the ETL deploy that holds the RAD-400 fix, with radiant-case-scheduler paused and no radiant-import,
-- import_part or radiant-reannotate-open-data run active. Then run the frequency recompute (see the upgrade note).


-- ---------------------------------------------------------------------------------------------------
-- 0. Read-only, per tenant. 0a and 0b empty for every tenant: nothing to do.
-- ---------------------------------------------------------------------------------------------------

-- 0a. The germline orphans, per part.
SELECT o.part, COUNT(*) AS n_occurrences, GROUP_CONCAT(DISTINCT CAST(o.task_id AS VARCHAR)) AS task_ids
FROM <tenant>_tenant.germline__snv__occurrence o
LEFT ANTI JOIN (SELECT DISTINCT task_id FROM <shared_db>.staging_sequencing_experiment) s ON s.task_id = o.task_id
GROUP BY o.part;

-- 0b. The somatic orphans, per part.
SELECT o.part, COUNT(*) AS n_occurrences, GROUP_CONCAT(DISTINCT CAST(o.task_id AS VARCHAR)) AS task_ids
FROM <tenant>_tenant.somatic__snv__occurrence o
LEFT ANTI JOIN (SELECT DISTINCT task_id FROM <shared_db>.staging_sequencing_experiment) s ON s.task_id = o.task_id
GROUP BY o.part;


-- ---------------------------------------------------------------------------------------------------
-- 1. Per tenant, when 0a or 0b is not empty: delete the orphans. Both are Duplicate Key tables, which take no
--    subquery in a DELETE: paste the lists. The Iceberg tables need nothing, the import reads them by task.
-- ---------------------------------------------------------------------------------------------------

DELETE FROM <tenant>_tenant.germline__snv__occurrence
WHERE task_id IN (<germline_task_ids>);

DELETE FROM <tenant>_tenant.somatic__snv__occurrence
WHERE task_id IN (<somatic_task_ids>);

--
--   Re-run 0a and 0b: they return nothing.
--


-- ---------------------------------------------------------------------------------------------------
-- 2. Per tenant of section 1: delete the staging frequencies of its parts. The recompute visits only the parts that
--    still have staging rows, and writes their frequencies back. A part left with no task would keep its old rows.
-- ---------------------------------------------------------------------------------------------------

DELETE FROM <shared_db>.germline__snv__staging_variant_frequency_part
WHERE tenant_code = '<tenant>' AND part IN (<parts>);

DELETE FROM <shared_db>.somatic__snv__staging_variant_frequency_part
WHERE tenant_code = '<tenant>' AND part IN (<parts>);


-- ---------------------------------------------------------------------------------------------------
-- 3. Per tenant of section 1: delete its variant parts. The recompute writes back those that still have staging rows.
-- ---------------------------------------------------------------------------------------------------

DELETE FROM <tenant>_tenant.snv__variant_partitioned
WHERE part IN (<variant_parts>);


-- ---------------------------------------------------------------------------------------------------
-- 4. The consequence filter is shared by every tenant, and the recompute does not rewrite it: delete only the parts
--    that no tenant has an occurrence in any more. 0c, per tenant, after section 1: the parts that still have one.
-- ---------------------------------------------------------------------------------------------------

-- 0c.
SELECT DISTINCT part FROM <tenant>_tenant.germline__snv__occurrence WHERE part IN (<parts>)
UNION
SELECT DISTINCT part FROM <tenant>_tenant.somatic__snv__occurrence WHERE part IN (<parts>);

DELETE FROM <shared_db>.snv__consequence_filter_partitioned
WHERE part IN (<empty_parts>);

--
--   A part that still has occurrences keeps the filter rows of the purged loci until its next import. The portal
--   reaches them only through the occurrences, which are gone.
--
