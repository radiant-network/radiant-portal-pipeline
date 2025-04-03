CREATE TABLE IF NOT EXISTS `dbnsfp` (
  `locus_id` bigint(20) NULL COMMENT "",
  `ensembl_transcript_id` varchar(1048576) NULL COMMENT "",
  `sift_score` decimal(38, 9) NULL COMMENT "",
  `sift_pred` varchar(1048576) NULL COMMENT "",
  `polyphen2_hvar_score` decimal(38, 9) NULL COMMENT "",
  `polyphen2_hvar_pred` varchar(1048576) NULL COMMENT "",
  `fathmm_score` decimal(38, 9) NULL COMMENT "",
  `fathmm_pred` varchar(1048576) NULL COMMENT "",
  `cadd_score` decimal(38, 9) NULL COMMENT "",
  `cadd_phred` decimal(38, 9) NULL COMMENT "",
  `dann_score` decimal(38, 9) NULL COMMENT "",
  `revel_score` decimal(38, 9) NULL COMMENT "",
  `lrt_score` decimal(38, 9) NULL COMMENT "",
  `lrt_pred` varchar(1048576) NULL COMMENT "",
  `phyloP17way_primate` decimal(38, 9) NULL COMMENT "",
  `phyloP100way_vertebrate` decimal(38, 9) NULL COMMENT ""
)
ENGINE=OLAP
DUPLICATE KEY(`locus_id`, `ensembl_transcript_id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "query_group"
);