CREATE TABLE IF NOT EXISTS {{ params.starrocks_dbsnp }} (
  `locus_id` bigint(20) NOT NULL COMMENT "",
  `rsnumber` varchar(20) NULL COMMENT ""
)
ENGINE=OLAP
DUPLICATE KEY(`locus_id`, `rsnumber`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ params.colocate_query_group }}"
);