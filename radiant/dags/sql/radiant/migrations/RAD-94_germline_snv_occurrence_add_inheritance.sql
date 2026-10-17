-- RAD-94 (RAD-93 task 1) — parental origin and transmission mode read from the slivar tags.
--
--   other_transmission_modes      ARRAY<VARCHAR(50)>   the slivar modes not kept in transmission_mode
--   transmission_mode_candidate   BOOLEAN              true when slivar flags the sample `candidate`
--   parental_origin_source        VARCHAR(10)          'slivar' or 'etl': which method gave parental_origin
--   transmission_mode_source      VARCHAR(10)          'slivar' or 'etl': which method gave transmission_mode
--
-- The VCF header decides each field on its own: a `po_*` tag → parental origin from slivar, a mode tag with the Radiant
-- names (BIOINFO-239) → transmission mode from slivar; otherwise the ETL calculation, unchanged. The four columns are
-- set for the progenies of a family only, like parental_origin and transmission_mode.
--
-- Run manually, ONCE in every {tenant}_tenant database (germline__snv__occurrence is per tenant,
-- STARROCKS_RADIANT_PER_TENANT_MAPPING in radiant/tasks/data/radiant_tables.py). `USE` each database in turn.
-- A single-tenant deployment keeps the table in the base database.
--
-- The columns are appended at the end on purpose: the occurrence insert is a positional INSERT with no target
-- column list, and germline_snv_occurrence_copy_partition.sql copies `SELECT *`. The table must end up in
-- exactly the order declared by init/germline_snv_occurrence_create_table.sql, so run this AFTER
-- RAD-88_germline_snv_occurrence_add_comphet.sql.
--
-- New deployments get the columns from init/germline_snv_occurrence_create_table.sql and must NOT run this
-- script. StarRocks has no `ADD COLUMN IF NOT EXISTS` (checked on 4.0.16), so re-running it fails on the ALTER.
--
-- HARD PREREQUISITES for the deployment, BEFORE the new DAGs are deployed:
--   1. this script, in every {tenant}_tenant database. Against an unmigrated table the positional insert fails and
--      every import_part breaks;
--   2. the Iceberg `germline_snv_occurrence` table recreated with the new schema: run ONLY the germline SNV
--      occurrence task of the init-iceberg-tables DAG (the other tasks drop and recreate their own tables), with no
--      import running. The occurrence insert is its only reader, for the tasks being imported, so the dropped history
--      has no reader.
--
-- Let the ALTER reach FINISHED in a database before the next import runs there.
--
-- No backfill: the occurrences imported before keep the four columns NULL until their case is re-imported. A NULL
-- source means "imported before RAD-94, or not a progeny of a family".


-- ---------------------------------------------------------------------------------------------------
-- Every {tenant}_tenant database.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE germline__snv__occurrence ADD COLUMN (
    other_transmission_modes ARRAY<VARCHAR(50)> NULL COMMENT "",
    transmission_mode_candidate BOOLEAN NULL COMMENT "",
    parental_origin_source VARCHAR(10) NULL COMMENT "",
    transmission_mode_source VARCHAR(10) NULL COMMENT ""
);


-- ---------------------------------------------------------------------------------------------------
-- Post-checks, read-only.
-- ---------------------------------------------------------------------------------------------------
--
--   SHOW ALTER TABLE COLUMN FROM <database>;
--       -- expect State = FINISHED
--
--   DESC germline__snv__occurrence;
--       -- the four columns must be the last ones, after comphet_genes.
--
-- After the next import of a family case:
--
--   SELECT parental_origin_source, transmission_mode_source, count(*)
--   FROM germline__snv__occurrence
--   WHERE parental_origin_source IS NOT NULL
--   GROUP BY 1, 2;
