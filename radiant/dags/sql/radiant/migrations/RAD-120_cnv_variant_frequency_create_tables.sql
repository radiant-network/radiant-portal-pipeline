-- In-cohort CNV frequencies per tenant: the two tables that import_part fills after the CNV occurrence inserts.
--
--   germline__cnv__variant_frequency   germline_cnv_variant_frequency_insert.sql
--   somatic__cnv__variant_frequency    somatic_cnv_variant_frequency_insert.sql
--
-- Design: design/RAD-120-cnv-cohort-frequency.md.
--
-- Run manually, ONCE in every {tenant}_tenant database (both tables are per tenant,
-- STARROCKS_RADIANT_PER_TENANT_MAPPING in radiant/tasks/data/radiant_tables.py). `USE` each database in turn.
-- A single-tenant deployment keeps the tables in the base database.
--
-- WHY. import_radiant creates the per-tenant tables only for the tenants of its batch (prepare_tenants_tables). A
-- tenant with no import since the deploy has neither table, and the data QA run fails on it: the tables are dbt
-- sources, and somatic__cnv__variant_frequency is in the `absent_tables` of the tenants with no somatic data
-- (TENANT_QA_SHAPES in radiant/dags/data_integrity_starrocks.py), so
-- tenant_qa_shape__validate_absent_tables_are_empty counts the rows of a table that does not exist.
--
-- The statements are identical to init/germline_cnv_variant_frequency_create_table.sql and
-- init/somatic_cnv_variant_frequency_create_table.sql, with the database left to `USE`. `IF NOT EXISTS` makes the
-- script safe to re-run, and safe in a database that an import has already prepared.
--
-- No backfill: the tables stay empty in a tenant until its next import_part run recomputes them. Until then the
-- data QA `should_not_be_empty` tests of the germline table fail on that tenant.


-- ---------------------------------------------------------------------------------------------------
-- Every {tenant}_tenant database.
-- ---------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS germline__cnv__variant_frequency (
   `cnv_id` BIGINT NOT NULL,
   `pc_wgs` BIGINT,
   `pn_wgs` BIGINT,
   `pf_wgs` DOUBLE,
   `pc_wgs_affected` BIGINT,
   `pn_wgs_affected` BIGINT,
   `pf_wgs_affected` DOUBLE,
   `pc_wgs_not_affected` BIGINT,
   `pn_wgs_not_affected` BIGINT,
   `pf_wgs_not_affected` DOUBLE,
   `pc_wxs` BIGINT,
   `pn_wxs` BIGINT,
   `pf_wxs` DOUBLE,
   `pc_wxs_affected` BIGINT,
   `pn_wxs_affected` BIGINT,
   `pf_wxs_affected` DOUBLE,
   `pc_wxs_not_affected` BIGINT,
   `pn_wxs_not_affected` BIGINT,
   `pf_wxs_not_affected` DOUBLE
)
DISTRIBUTED BY HASH(`cnv_id`)
BUCKETS 10;

CREATE TABLE IF NOT EXISTS somatic__cnv__variant_frequency (
   `cnv_id` BIGINT NOT NULL,
   `pc_to_wgs` BIGINT,
   `pn_to_wgs` BIGINT,
   `pf_to_wgs` DOUBLE,
   `pc_to_wxs` BIGINT,
   `pn_to_wxs` BIGINT,
   `pf_to_wxs` DOUBLE
)
DISTRIBUTED BY HASH(`cnv_id`)
BUCKETS 10;


-- ---------------------------------------------------------------------------------------------------
-- Post-checks, read-only.
-- ---------------------------------------------------------------------------------------------------
--
--   SHOW TABLES LIKE '%cnv__variant_frequency';
--       -- germline__cnv__variant_frequency and somatic__cnv__variant_frequency
--
-- After the next import_part run of the tenant:
--
--   SELECT count(*) AS should_be_non_zero FROM germline__cnv__variant_frequency;
