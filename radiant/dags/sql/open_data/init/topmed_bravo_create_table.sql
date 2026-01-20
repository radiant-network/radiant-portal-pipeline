CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_topmed_bravo }} (
  `locus_id` bigint(20) NOT NULL COMMENT "",
  `af` double COMMENT "",
  `ac` INT(11) COMMENT "",
  `an` INT(11) COMMENT "",
  `hom` INT(11) COMMENT ""
)
ENGINE=OLAP
DUPLICATE KEY(`locus_id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ mapping.colocate_query_group }}"
);