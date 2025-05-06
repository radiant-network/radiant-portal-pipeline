CREATE TABLE IF NOT EXISTS {{ params.starrocks_staging_variants }}
(
    `locus_id` BIGINT NOT NULL,
    `chromosome` char(2),
    `start` bigint NULL COMMENT "",
    `variant_class` varchar(50) NULL COMMENT "",
    `symbol` varchar(20) NULL COMMENT "",
    `impact_score` tinyint NULL COMMENT "",
    `consequences` array< varchar (100)> NULL COMMENT "",
    `vep_impact` varchar(20) NULL COMMENT "",
    `is_mane_select` boolean NULL COMMENT "",
    `is_mane_plus` boolean NULL COMMENT "",
    `is_canonical` boolean NULL COMMENT "",
    `rsnumber` array< varchar (15)> NULL COMMENT "",
    `end` bigint NULL COMMENT "",
    `reference` varchar(2000),
    `alternate` varchar(2000),
    `mane_select` varchar(200) NULL,
    `hgvsg` varchar(2000) NULL,
    `hgvsc` varchar(2000) NULL,
    `hgvsp` varchar(2000) NULL,
    `locus` varchar(2000) NULL,
    `locus_hash` varchar(64) NULL,
    `dna_change` varchar(2000),
    `aa_change` varchar(2000)
)
ENGINE=OLAP
PRIMARY KEY(`locus_id`)
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ params.colocate_query_group }}"
);