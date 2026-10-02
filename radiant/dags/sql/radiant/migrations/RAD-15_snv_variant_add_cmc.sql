-- RAD-15 / RAD-16 — denormalise the COSMIC Mutation Census fields into the variant tables.
--
-- Four columns, filled by snv_staging_variant_insert.sql and snv_staging_variant_reannotate.sql from
-- cosmic_mutation_set (on locus_id) or, when the locus is not there, from cosmic_mutation_set_hgvs (on the
-- picked transcript_id + dna_change), then carried to snv__variant and snv__variant_partitioned:
--
--   cmc_mutation_url    VARCHAR(255)   cosmic_mutation_set[_hgvs].mutation_url
--   cmc_sample_mutated  INT            .sample_mutated
--   cmc_sample_ratio    DOUBLE         .sample_ratio
--   cmc_tier            VARCHAR(8)     .tier ('1', '2', '3', 'Other')
--
-- Run manually, ONCE per database. The migration spans both scopes (radiant/tasks/data/radiant_tables.py):
--
--   base database           snv__staging_variant                     (STARROCKS_RADIANT_BASE_MAPPING)
--   every {tenant}_tenant   snv__variant, snv__variant_partitioned   (STARROCKS_RADIANT_PER_TENANT_MAPPING)
--
-- `USE` each database in turn and run only its block. A single-tenant deployment keeps all three in the
-- base database. snv__tmp_variant is not touched: the CMC fields are joined in at the staging step.
--
-- The columns are appended at the end on purpose: snv_staging_variant_insert.sql and snv_variant_insert.sql
-- are positional INSERTs with no target column list, and snv_variant_part_insert_part.sql copies `v.*` from
-- `snv__variant` into `snv__variant_partitioned`. All three tables must end up in exactly the order declared
-- by their init/snv_*variant*_create_table.sql, with these four columns after omim_inheritance_code.
--
-- New deployments get the columns from init/*_create_table.sql and must NOT run this script. StarRocks has
-- no `ADD COLUMN IF NOT EXISTS` (3.4.2), so re-running it fails on the ALTER.
--
-- HARD PREREQUISITE for the deployment: run this BEFORE the new DAGs are deployed. Against an unmigrated
-- snv__staging_variant the positional insert fails and every import_part breaks.
--
-- Issue each ALTER separately and let it reach FINISHED before the next: StarRocks rejects a second ALTER
-- while the table state is not NORMAL. Each one is a metadata-only light schema change.
--
-- No UPDATE: existing rows stay NULL until a forced re-annotation fills them
-- (reannotate_open_data with force_reannotation=True), which rewrites snv__staging_variant, then
-- snv__variant, then every part of snv__variant_partitioned. New imports get the values straight away.


-- ---------------------------------------------------------------------------------------------------
-- Base database.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE snv__staging_variant ADD COLUMN (
    cmc_mutation_url VARCHAR(255) NULL COMMENT "",
    cmc_sample_mutated INT NULL COMMENT "",
    cmc_sample_ratio DOUBLE NULL COMMENT "",
    cmc_tier VARCHAR(8) NULL COMMENT ""
);


-- ---------------------------------------------------------------------------------------------------
-- Every {tenant}_tenant database.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE snv__variant ADD COLUMN (
    cmc_mutation_url VARCHAR(255) NULL COMMENT "",
    cmc_sample_mutated INT NULL COMMENT "",
    cmc_sample_ratio DOUBLE NULL COMMENT "",
    cmc_tier VARCHAR(8) NULL COMMENT ""
);

ALTER TABLE snv__variant_partitioned ADD COLUMN (
    cmc_mutation_url VARCHAR(255) NULL COMMENT "",
    cmc_sample_mutated INT NULL COMMENT "",
    cmc_sample_ratio DOUBLE NULL COMMENT "",
    cmc_tier VARCHAR(8) NULL COMMENT ""
);


-- ---------------------------------------------------------------------------------------------------
-- Post-checks, read-only.
-- ---------------------------------------------------------------------------------------------------
--
--   SHOW ALTER TABLE COLUMN FROM <database>;
--       -- expect State = FINISHED for every statement
--
--   DESC snv__variant;
--       -- the four cmc_* columns must be the last ones, after omim_inheritance_code, in the order above.
--       -- Same check on snv__staging_variant and snv__variant_partitioned.
--
-- After the forced re-annotation:
--
--   SELECT cmc_tier, count(*) FROM snv__variant GROUP BY 1;
--       -- expect '1', '2', '3', 'Other' and a large NULL bucket (most variants are not in COSMIC).
--       -- Only NULL means the re-annotation did not run or cosmic_mutation_set is empty.
--
--   SELECT count(*) AS should_be_zero FROM snv__variant
--    WHERE cmc_sample_ratio < 0 OR cmc_sample_ratio > 1;
