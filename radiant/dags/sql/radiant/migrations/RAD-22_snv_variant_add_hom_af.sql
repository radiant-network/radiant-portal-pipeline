-- RAD-22 / RAD-45 — homozygous count and approximate allele frequency in the variant tables.
--
-- `hom` is the number of distinct patients called HOM or HEM (`zygosity` for germline, `tumor_zygosity` for
-- somatic), with the same filters and cohorts as `pc`. It is counted per part by
-- *_snv_staging_variant_freq_insert.sql, summed per tenant by *_snv_variant_frequency_insert.sql, and
-- snv_variant_insert.sql derives `af = (pc + hom) / (2 * pn)` (0 when pn = 0) next to `pf`:
--
--   germline frequency tables   hom_{wgs,wxs}{,_affected,_not_affected}            BIGINT   6 columns
--   somatic frequency tables    hom_{tn,to}_{wgs,wxs}                              BIGINT   4 columns
--   snv__variant[_partitioned]  germline_{hom,af}_*, somatic_{hom,af}_*      INT(11)/DOUBLE  20 columns
--
-- Run manually, ONCE per database. The migration spans both scopes (radiant/tasks/data/radiant_tables.py):
--
--   base database           germline__snv__staging_variant_frequency_part,     (STARROCKS_RADIANT_BASE_MAPPING)
--                           somatic__snv__staging_variant_frequency_part
--   every {tenant}_tenant   germline__snv__variant_frequency,                  (STARROCKS_RADIANT_PER_TENANT_MAPPING)
--                           somatic__snv__variant_frequency,
--                           snv__variant, snv__variant_partitioned
--
-- `USE` each database in turn and run only its block. A single-tenant deployment keeps all six in the base
-- database.
--
-- The columns are appended at the end on purpose: every frequency insert and snv_variant_insert.sql are
-- positional INSERTs with no target column list, and snv_variant_part_insert_part.sql copies `v.*` from
-- `snv__variant` into `snv__variant_partitioned`. All six tables must end up in exactly the order declared by
-- their init/*_create_table.sql.
--
-- New deployments get the columns from init/*_create_table.sql and must NOT run this script. StarRocks has
-- no `ADD COLUMN IF NOT EXISTS` (3.4.2), so re-running it fails on the ALTER.
--
-- HARD PREREQUISITE for the deployment: run this BEFORE the new DAGs are deployed. Against an unmigrated
-- table the positional inserts fail and every import_part breaks.
--
-- Issue each ALTER separately and let it reach FINISHED before the next: StarRocks rejects a second ALTER
-- while the table state is not NORMAL. Measure the duration on QA before prod, on snv__variant_partitioned
-- and the staging frequency tables in particular.
--
-- No UPDATE: the new columns start NULL. Parts imported after the migration get their `hom` straight away,
-- but the parts imported before keep a NULL `hom` in the staging frequency tables, so the tenant roll-up
-- and snv__variant under-count `hom` and `af` is too low, with no error, until the frequencies are
-- recomputed (reannotate_open_data with recompute_frequencies, RAD-46).


-- ---------------------------------------------------------------------------------------------------
-- Base database.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE germline__snv__staging_variant_frequency_part ADD COLUMN (
    hom_wgs BIGINT NULL COMMENT "",
    hom_wgs_affected BIGINT NULL COMMENT "",
    hom_wgs_not_affected BIGINT NULL COMMENT "",
    hom_wxs BIGINT NULL COMMENT "",
    hom_wxs_affected BIGINT NULL COMMENT "",
    hom_wxs_not_affected BIGINT NULL COMMENT ""
);

ALTER TABLE somatic__snv__staging_variant_frequency_part ADD COLUMN (
    hom_tn_wgs BIGINT NULL COMMENT "",
    hom_tn_wxs BIGINT NULL COMMENT "",
    hom_to_wgs BIGINT NULL COMMENT "",
    hom_to_wxs BIGINT NULL COMMENT ""
);


-- ---------------------------------------------------------------------------------------------------
-- Every {tenant}_tenant database.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE germline__snv__variant_frequency ADD COLUMN (
    hom_wgs BIGINT NULL COMMENT "",
    hom_wgs_affected BIGINT NULL COMMENT "",
    hom_wgs_not_affected BIGINT NULL COMMENT "",
    hom_wxs BIGINT NULL COMMENT "",
    hom_wxs_affected BIGINT NULL COMMENT "",
    hom_wxs_not_affected BIGINT NULL COMMENT ""
);

ALTER TABLE somatic__snv__variant_frequency ADD COLUMN (
    hom_tn_wgs BIGINT NULL COMMENT "",
    hom_tn_wxs BIGINT NULL COMMENT "",
    hom_to_wgs BIGINT NULL COMMENT "",
    hom_to_wxs BIGINT NULL COMMENT ""
);

ALTER TABLE snv__variant ADD COLUMN (
    germline_hom_wgs INT(11) NULL COMMENT "",
    germline_af_wgs DOUBLE NULL COMMENT "",
    germline_hom_wgs_affected INT(11) NULL COMMENT "",
    germline_af_wgs_affected DOUBLE NULL COMMENT "",
    germline_hom_wgs_not_affected INT(11) NULL COMMENT "",
    germline_af_wgs_not_affected DOUBLE NULL COMMENT "",
    germline_hom_wxs INT(11) NULL COMMENT "",
    germline_af_wxs DOUBLE NULL COMMENT "",
    germline_hom_wxs_affected INT(11) NULL COMMENT "",
    germline_af_wxs_affected DOUBLE NULL COMMENT "",
    germline_hom_wxs_not_affected INT(11) NULL COMMENT "",
    germline_af_wxs_not_affected DOUBLE NULL COMMENT "",
    somatic_hom_tn_wgs INT(11) NULL COMMENT "",
    somatic_af_tn_wgs DOUBLE NULL COMMENT "",
    somatic_hom_tn_wxs INT(11) NULL COMMENT "",
    somatic_af_tn_wxs DOUBLE NULL COMMENT "",
    somatic_hom_to_wgs INT(11) NULL COMMENT "",
    somatic_af_to_wgs DOUBLE NULL COMMENT "",
    somatic_hom_to_wxs INT(11) NULL COMMENT "",
    somatic_af_to_wxs DOUBLE NULL COMMENT ""
);

ALTER TABLE snv__variant_partitioned ADD COLUMN (
    germline_hom_wgs INT(11) NULL COMMENT "",
    germline_af_wgs DOUBLE NULL COMMENT "",
    germline_hom_wgs_affected INT(11) NULL COMMENT "",
    germline_af_wgs_affected DOUBLE NULL COMMENT "",
    germline_hom_wgs_not_affected INT(11) NULL COMMENT "",
    germline_af_wgs_not_affected DOUBLE NULL COMMENT "",
    germline_hom_wxs INT(11) NULL COMMENT "",
    germline_af_wxs DOUBLE NULL COMMENT "",
    germline_hom_wxs_affected INT(11) NULL COMMENT "",
    germline_af_wxs_affected DOUBLE NULL COMMENT "",
    germline_hom_wxs_not_affected INT(11) NULL COMMENT "",
    germline_af_wxs_not_affected DOUBLE NULL COMMENT "",
    somatic_hom_tn_wgs INT(11) NULL COMMENT "",
    somatic_af_tn_wgs DOUBLE NULL COMMENT "",
    somatic_hom_tn_wxs INT(11) NULL COMMENT "",
    somatic_af_tn_wxs DOUBLE NULL COMMENT "",
    somatic_hom_to_wgs INT(11) NULL COMMENT "",
    somatic_af_to_wgs DOUBLE NULL COMMENT "",
    somatic_hom_to_wxs INT(11) NULL COMMENT "",
    somatic_af_to_wxs DOUBLE NULL COMMENT ""
);


-- ---------------------------------------------------------------------------------------------------
-- Post-checks, read-only.
-- ---------------------------------------------------------------------------------------------------
--
--   SHOW ALTER TABLE COLUMN FROM <database>;
--       -- expect State = FINISHED for every statement
--
--   DESC snv__variant;
--       -- the 20 new columns must be the last ones, after cmc_tier, in the order above. Same check on
--       -- snv__variant_partitioned, and on the four frequency tables (hom_* last).
--
-- After the frequency recompute (RAD-46):
--
--   SELECT count(*) AS should_be_zero FROM snv__variant
--    WHERE germline_hom_wgs > germline_pc_wgs OR germline_hom_wxs > germline_pc_wxs
--       OR somatic_hom_tn_wgs > somatic_pc_tn_wgs OR somatic_hom_to_wgs > somatic_pc_to_wgs;
--
--   SELECT count(*) AS should_be_zero FROM snv__variant
--    WHERE germline_af_wgs NOT BETWEEN 0 AND 1 OR germline_af_wxs NOT BETWEEN 0 AND 1;
--
--   SELECT count(*) AS should_be_non_zero FROM snv__variant WHERE germline_hom_wgs > 0;
--       -- 0 means the recompute did not run.
