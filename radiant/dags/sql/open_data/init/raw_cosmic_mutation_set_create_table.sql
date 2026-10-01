-- Staging table for the COSMIC Mutation Census: one row per (mutation, transcript) as published, with the
-- variant key rebuilt by radiant-import-cosmic-mutation-set (anchor base added, left-aligned with bcftools).
-- The key columns are NULL for the rows the normalizer could not place on GRCh38 (no coordinate, reference
-- allele contradicted by the FASTA); those rows feed cosmic_mutation_set_hgvs instead of cosmic_mutation_set.
-- Truncated and reloaded on every import; both derived tables are rebuilt from it.
CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_raw_cosmic_mutation_set }}
(
    `cosmic_id`      VARCHAR(32)    NOT NULL,
    `transcript_id`  VARCHAR(32)    NOT NULL,
    `locus_hash`     VARCHAR(64)    NULL,
    `chromosome`     VARCHAR(10)    NULL,
    `start`          BIGINT         NULL,
    `reference`      VARCHAR(65533) NULL,
    `alternate`      VARCHAR(65533) NULL,
    `mutation_url`   VARCHAR(255)   NULL,
    `shared_aa`      INT            NULL,
    `sample_mutated` INT            NULL,
    `sample_tested`  INT            NULL,
    `tier`           VARCHAR(8)     NULL,
    `cds_change`     VARCHAR(2000)  NOT NULL
)
ENGINE = OLAP
DUPLICATE KEY(`cosmic_id`, `transcript_id`)
DISTRIBUTED BY HASH(`cosmic_id`)
BUCKETS 10;
