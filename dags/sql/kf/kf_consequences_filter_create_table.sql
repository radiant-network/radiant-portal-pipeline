CREATE TABLE IF NOT EXISTS `consequences_filter` (
  `part` tinyint(4) NOT NULL COMMENT "",
  `locus_id` bigint(20) NULL COMMENT "",
  `is_deleterious` boolean NULL COMMENT "",
  `impact_score` tinyint(4) NULL COMMENT "",
  `symbol` varchar(30) NULL COMMENT "",
  `consequence` varchar(50) NULL COMMENT "",
  `biotype` varchar(50) NULL COMMENT "",
  `spliceai_ds` decimal(6, 5) NULL COMMENT "",
  `sift_score` decimal(6, 4) NULL COMMENT "",
  `sift_pred` varchar(1) NULL COMMENT "",
  `polyphen2_hvar_score` decimal(6, 5) NULL COMMENT "",
  `polyphen2_hvar_pred` varchar(1) NULL COMMENT "",
  `fathmm_score` decimal(6, 4) NULL COMMENT "",
  `fathmm_pred` varchar(1) NULL COMMENT "",
  `cadd_score` decimal(6, 4) NULL COMMENT "",
  `cadd_phred` decimal(6, 4) NULL COMMENT "",
  `dann_score` decimal(6, 5) NULL COMMENT "",
  `revel_score` decimal(6, 5) NULL COMMENT "",
  `lrt_score` decimal(6, 5) NULL COMMENT "",
  `lrt_pred` varchar(1) NULL COMMENT "",
  `phyloP17way_primate` decimal(7, 5) NULL COMMENT "",
  `phyloP100way_vertebrate` decimal(7, 5) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`part`, `locus_id`, `is_deleterious`)
COMMENT "OLAP"
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES ("colocate_with" = "query_group");