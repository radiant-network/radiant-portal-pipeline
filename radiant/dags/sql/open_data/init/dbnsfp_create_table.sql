CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_dbnsfp }} (
  `locus_id` bigint(20) NOT NULL COMMENT "",
  `ensembl_transcript_id` varchar(1048576) NULL COMMENT "",
  `sift_score` float NULL COMMENT "",
  `sift_pred` varchar(1) NULL COMMENT "",
  `polyphen2_hvar_score` float NULL COMMENT "",
  `polyphen2_hvar_pred` varchar(1) NULL COMMENT "",
  `fathmm_score` float NULL COMMENT "",
  `fathmm_pred` varchar(1) NULL COMMENT "",
  `cadd_score` float NULL COMMENT "",
  `cadd_phred` float NULL COMMENT "",
  `dann_score` float NULL COMMENT "",
  `revel_score` float NULL COMMENT "",
  `lrt_score` float NULL COMMENT "",
  `lrt_pred` varchar(1) NULL COMMENT "",
  `phyloP17way_primate` float NULL COMMENT "",
  `phyloP100way_vertebrate` float NULL COMMENT ""
)
ENGINE=OLAP
DUPLICATE KEY(`locus_id`, `ensembl_transcript_id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ mapping.colocate_query_group }}"
);