CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_gnomad_genomes_v3 }} (
  `locus_id` bigint(20) NOT NULL COMMENT "",
  `af` double NULL COMMENT ""
)
ENGINE=OLAP
DUPLICATE KEY(`locus_id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ mapping.colocate_query_group }}"
)