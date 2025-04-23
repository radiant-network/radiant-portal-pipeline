CREATE TABLE IF NOT EXISTS stg_variants
(
    `locus_id` BIGINT NOT NULL,
    `chromosome` char(2),
    `start` bigint NULL COMMENT "",
    `variant_class` varchar(50) NULL COMMENT "",
    `symbol` varchar(20) NULL COMMENT "",
    `consequence` array< varchar (50)> NULL COMMENT "",
    `vep_impact` varchar(20) NULL COMMENT "",
    `mane_select` boolean NULL COMMENT "",
    `mane_plus` boolean NULL COMMENT "",
    `canonical` boolean NULL COMMENT "",
    `picked` boolean NULL COMMENT "",
    `rsnumber` array< varchar (15)> NULL COMMENT "",
    `reference` varchar(2000),
    `alternate` varchar(2000),
    `hgvsg` varchar(2000) NULL,
    `locus` varchar(2000) NULL,
    `hash` varchar(64) NULL,
    `dna_change` varchar(2000),
    `aa_change` varchar(2000)
)
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "query_group"
);