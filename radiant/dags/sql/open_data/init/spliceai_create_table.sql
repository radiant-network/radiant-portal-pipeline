CREATE TABLE IF NOT EXISTS {{ params.starrocks_spliceai }} (
  `locus_id` bigint(20) NOT NULL COMMENT "",
  `symbol` varchar(1048576) NULL COMMENT "",
  `spliceai_ds` double NULL COMMENT "",
  `spliceai_type` array<varchar(1048576)> NULL COMMENT ""
)
ENGINE=OLAP
DUPLICATE KEY(`locus_id`, `symbol`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ params.colocate_query_group }}"
);