-- RAD-88 (RAD-39 task 2) — compound-het summary on the germline SNV occurrence.
--
--   has_comphet     BOOLEAN                                       true when the occurrence has a slivar
--                                                                 compound-het partner, false otherwise
--   comphet_genes   ARRAY<STRUCT<symbol VARCHAR(200), nb INT>>    one entry per gene, with the number of
--                                                                 partners in germline__snv__comphet
--
-- germline_snv_occurrence_insert_partition_delta.sql fills both from germline__snv__comphet (RAD-87), which
-- import_part loads first.
--
-- Run manually, ONCE in every {tenant}_tenant database (germline__snv__occurrence is per tenant,
-- STARROCKS_RADIANT_PER_TENANT_MAPPING in radiant/tasks/data/radiant_tables.py). `USE` each database in turn.
-- A single-tenant deployment keeps the table in the base database.
--
-- The columns are appended at the end on purpose: the occurrence insert is a positional INSERT with no target
-- column list, and germline_snv_occurrence_copy_partition.sql copies `SELECT *`. The table must end up in
-- exactly the order declared by init/germline_snv_occurrence_create_table.sql.
--
-- New deployments get the columns from init/germline_snv_occurrence_create_table.sql and must NOT run this
-- script. StarRocks has no `ADD COLUMN IF NOT EXISTS` (checked on 4.0.16), so re-running it fails on the ALTER.
--
-- HARD PREREQUISITE for the deployment: run this BEFORE the new DAGs are deployed. Against an unmigrated
-- table the positional insert fails and every import_part breaks.
--
-- Let the ALTER reach FINISHED in a database before the next import runs there.
--
-- No backfill: the occurrences imported before keep both columns NULL until their case is re-imported. A
-- NULL has_comphet means "not computed", false means "no compound het".


-- ---------------------------------------------------------------------------------------------------
-- Every {tenant}_tenant database.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE germline__snv__occurrence ADD COLUMN (
    has_comphet BOOLEAN NULL COMMENT "",
    comphet_genes ARRAY<STRUCT<symbol VARCHAR(200), nb INT>> NULL COMMENT ""
);


-- ---------------------------------------------------------------------------------------------------
-- Post-checks, read-only.
-- ---------------------------------------------------------------------------------------------------
--
--   SHOW ALTER TABLE COLUMN FROM <database>;
--       -- expect State = FINISHED
--
--   DESC germline__snv__occurrence;
--       -- has_comphet and comphet_genes must be the last two columns, after exomiser_gene_combined_score.
--
-- After the next import of a family case:
--
--   SELECT count(*) AS should_be_non_zero FROM germline__snv__occurrence WHERE has_comphet;
