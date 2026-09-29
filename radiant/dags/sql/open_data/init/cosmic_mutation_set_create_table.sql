-- COSMIC Mutation Census, one row per variant: the `cmc` shape of the legacy Spark ETL
-- (datalake-lib Variants.scala withCosmic), deduplicated per locus keeping the highest sample_mutated.
CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_cosmic_mutation_set }}
(
    `locus_id`       BIGINT(20)   NOT NULL,
    `mutation_url`   VARCHAR(255) NULL,
    `shared_aa`      INT          NULL,
    `cosmic_id`      VARCHAR(32)  NULL,
    `sample_mutated` INT          NULL,
    `sample_tested`  INT          NULL,
    `tier`           VARCHAR(8)   NULL,
    `sample_ratio`   DOUBLE       NULL
)
ENGINE = OLAP
DUPLICATE KEY(`locus_id`)
DISTRIBUTED BY HASH(`locus_id`)
BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ mapping.colocate_query_group }}"
);
