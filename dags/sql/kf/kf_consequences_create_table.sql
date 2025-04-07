CREATE TABLE IF NOT EXISTS `kf_consequences` (
    `locus_id` bigint(20) COMMENT "",
    `symbol` varchar(30) COMMENT "",
    `ensembl_transcript_id` varchar(100) COMMENT "",
    `consequences` array<varchar(50)> COMMENT "",
    `impact_score` tinyint(4) NULL COMMENT "",
    `biotype` varchar(50) NULL COMMENT "",
    `spliceai_ds` decimal(6, 5) NULL COMMENT "",
    `spliceai_type` array<varchar(2)> NULL COMMENT "",
    `canonical` boolean NULL COMMENT "",
    `picked` boolean NULL COMMENT "",
    `mane_select` boolean NULL COMMENT "",
    `mane_plus` boolean NULL COMMENT "",
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
    `phyloP100way_vertebrate` decimal(7, 5) NULL COMMENT "",
    `aa_change` varchar(1000) NULL COMMENT "",
    `coding_dna_change` varchar(1000) NULL COMMENT ""
)
ENGINE=OLAP
PRIMARY KEY(`locus_id`, `symbol`, `ensembl_transcript_id`)
DISTRIBUTED BY HASH(`locus_id`)
BUCKETS 10
PROPERTIES (
	"colocate_with" = "query_group"
);
