CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_clinvar }} (
  `locus_id` bigint(20) NOT NULL COMMENT "",
  `chromosome` varchar(1048576) NULL COMMENT "",
  `start` bigint(20) NULL COMMENT "",
  `end` bigint(20) NULL COMMENT "",
  `reference` varchar(1048576) NULL COMMENT "",
  `alternate` varchar(1048576) NULL COMMENT "",
  `interpretations` array<varchar(1048576)> NULL COMMENT "",
  `name` varchar(1048576) NULL COMMENT "",
  `clin_sig` array<varchar(1048576)> NULL COMMENT "",
  `clin_sig_conflict` array<varchar(1048576)> NULL COMMENT "",
  `af_exac` double NULL COMMENT "",
  `clnvcso` varchar(1048576) NULL COMMENT "",
  `geneinfo` varchar(1048576) NULL COMMENT "",
  `clnsigincl` array<varchar(1048576)> NULL COMMENT "",
  `clnvi` array<varchar(1048576)> NULL COMMENT "",
  `clndisdb` array<varchar(1048576)> NULL COMMENT "",
  `clnrevstat` array<varchar(1048576)> NULL COMMENT "",
  `alleleid` int(11) NULL COMMENT "",
  `origin` array<varchar(1048576)> NULL COMMENT "",
  `clndnincl` array<varchar(1048576)> NULL COMMENT "",
  `rs` array<varchar(1048576)> NULL COMMENT "",
  `dbvarid` array<varchar(1048576)> NULL COMMENT "",
  `af_tgp` double NULL COMMENT "",
  `clnvc` varchar(1048576) NULL COMMENT "",
  `clnhgvs` array<varchar(1048576)> NULL COMMENT "",
  `mc` array<varchar(1048576)> NULL COMMENT "",
  `af_esp` double NULL COMMENT "",
  `clndisdbincl` array<varchar(1048576)> NULL COMMENT "",
  `conditions` array<varchar(1048576)> NULL COMMENT "",
  `inheritance` array<varchar(1048576)> NULL COMMENT "",
  `locus` varchar(1048576) NULL COMMENT "",
  `locus_hash` varchar(1048576) NULL COMMENT ""
)
ENGINE=OLAP
DUPLICATE KEY(`locus_id`, `chromosome`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ mapping.colocate_query_group }}"
)
