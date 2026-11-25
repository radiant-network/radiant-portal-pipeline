-- RAD-315 — the gene coverage of the germline and tumor-only alignments.
--
-- Two new tables, filled by import_part (gene_coverage task group), from the DRAGEN coverage_by_gene CSV (`covgene`
-- document) of the current alignment of each experiment:
--
--   raw_gene_coverage   base database, shared by the tenants    staging_gene_coverage_load.sql
--   gene_coverage       every {tenant}_tenant database          gene_coverage_insert_partition_delta.sql
--
-- Design: radiant-hub docs/features/RAD-315-gene-coverage-germline-somatic-alignments.md.
--
-- The statements are identical to init/staging_gene_coverage_create_table.sql and init/gene_coverage_create_table.sql,
-- with the database left to `USE`. `IF NOT EXISTS` makes the script safe to re-run. No change to an existing table.
--
-- WHY. Without raw_gene_coverage, the load_gene_coverage_files task fails in every import_part run. import_radiant
-- creates the per-tenant tables only for the tenants of its batch (prepare_tenants_tables): a tenant with no import
-- since the deploy has no gene_coverage table.
--
-- WHERE. Section 1 in the base database (`USE <shared_db>`), once. Section 2 in every <tenant>_tenant database (`USE`
-- each one in turn); a single-tenant deployment runs it in the base database.
--
-- WHEN. Before the ETL deploy, with no radiant-import / import_part running.
--
-- No backfill: the coverage of a sequencing experiment loads at its next import (a case re-submitted by hand).


-- ---------------------------------------------------------------------------------------------------
-- 1. Base database (<shared_db>), once.
-- ---------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw_gene_coverage
(
    part             INT,
    seq_id           INT,
    task_id          INT,
    tenant_code      VARCHAR(50),
    gene             VARCHAR(200),
    type             VARCHAR(50),
    size             INT,
    average_coverage DOUBLE,
    coverage_5x      FLOAT,
    coverage_15x     FLOAT,
    coverage_30x     FLOAT,
    coverage_50x     FLOAT,
    coverage_100x    FLOAT,
    coverage_200x    FLOAT,
    coverage_300x    FLOAT,
    coverage_400x    FLOAT,
    coverage_500x    FLOAT,
    coverage_1000x   FLOAT
)
ENGINE = OLAP DUPLICATE KEY(`part`, `seq_id`, `task_id`)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`gene`) BUCKETS 10;


-- ---------------------------------------------------------------------------------------------------
-- 2. Every <tenant>_tenant database.
-- ---------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gene_coverage
(
    part             INT,
    seq_id           INT,
    task_id          INT,
    symbol           VARCHAR(200),
    chromosome       VARCHAR(10),
    gene             VARCHAR(200),
    ensembl_gene_id  VARCHAR(128),
    size             INT,
    average_coverage DOUBLE,
    coverage_5x      FLOAT,
    coverage_15x     FLOAT,
    coverage_30x     FLOAT,
    coverage_50x     FLOAT,
    coverage_100x    FLOAT,
    coverage_200x    FLOAT,
    coverage_300x    FLOAT,
    coverage_400x    FLOAT,
    coverage_500x    FLOAT,
    coverage_1000x   FLOAT
)
ENGINE = OLAP
DUPLICATE KEY(`part`, `seq_id`, `task_id`, `symbol`, `chromosome`)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`seq_id`)
BUCKETS 10;


-- ---------------------------------------------------------------------------------------------------
-- Post-checks, read-only.
-- ---------------------------------------------------------------------------------------------------
--
--   SHOW TABLES LIKE 'raw_gene_coverage';      -- base database: one row
--   SHOW TABLES LIKE 'gene_coverage';          -- each <tenant>_tenant database: one row
--
-- After the next import_part run of a part with a `covgene` file:
--
--   SELECT seq_id, task_id, COUNT(*) FROM gene_coverage GROUP BY seq_id, task_id;
--       -- about 20,000 rows per alignment (one per CDS gene)


-- ---------------------------------------------------------------------------------------------------
-- Rollback. Deploy the previous ETL version, then drop the tables.
-- ---------------------------------------------------------------------------------------------------

-- DROP TABLE IF EXISTS raw_gene_coverage;     -- base database
-- DROP TABLE IF EXISTS gene_coverage;         -- each <tenant>_tenant database
