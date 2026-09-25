-- Staging table for the COSMIC Mutation Census: one row per (mutation, transcript) as published, with the
-- variant key rebuilt by radiant-import-cosmic-mutation-set (anchor base added, left-aligned with bcftools).
-- Truncated and reloaded on every import; cosmic_mutation_set is derived from it.
CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_raw_cosmic_mutation_set }}
(
    `locus_hash`     VARCHAR(64)   NOT NULL,
    `chromosome`     VARCHAR(10)   NOT NULL,
    `start`          BIGINT        NOT NULL,
    `reference`      VARCHAR(2000) NOT NULL,
    `alternate`      VARCHAR(2000) NOT NULL,
    `mutation_url`   VARCHAR(255)  NULL,
    `shared_aa`      INT           NULL,
    `cosmic_id`      VARCHAR(32)   NULL,
    `sample_mutated` INT           NULL,
    `sample_tested`  INT           NULL,
    `tier`           VARCHAR(8)    NULL
)
ENGINE = OLAP
DUPLICATE KEY(`locus_hash`)
DISTRIBUTED BY HASH(`locus_hash`)
BUCKETS 10;
