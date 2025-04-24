CREATE TABLE IF NOT EXISTS `topmed_bravo` (
  `locus_id` bigint(20) NULL COMMENT "",
  `af` decimal(38, 9) NULL COMMENT ""
)
ENGINE=OLAP
DUPLICATE KEY(`locus_id`, `af`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "query_group"
);