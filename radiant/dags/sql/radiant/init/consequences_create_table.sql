CREATE TABLE IF NOT EXISTS {{ params.starrocks_consequences }} (
    `locus_id` bigint(20) COMMENT "",
    `symbol` varchar(30) COMMENT "",
    `transcript_id` varchar(100) COMMENT "",
    `consequences` array<varchar(50)> COMMENT "",
    `impact_score` tinyint(4) NULL COMMENT "",
    `biotype` varchar(50) NULL COMMENT "",
    `spliceai_ds` float NULL COMMENT "",
    `spliceai_type` array<varchar(2)> NULL COMMENT "",
    `is_canonical` boolean NULL COMMENT "",
    `is_picked` boolean NULL COMMENT "",
    `is_mane_select` boolean NULL COMMENT "",
    `is_mane_plus` boolean NULL COMMENT "",
    `mane_select` varchar(200) NULL COMMENT "",
    `sift_score` float NULL COMMENT "",
    `sift_pred` varchar(1) NULL COMMENT "",
    `polyphen2_hvar_score` float NULL COMMENT "",
    `polyphen2_hvar_pred` varchar(1) NULL COMMENT "",
    `fathmm_score` float NULL COMMENT "",
    `fathmm_pred` varchar(1) NULL COMMENT "",
    `cadd_score` float NULL COMMENT "",
    `cadd_phred` float NULL COMMENT "",
    `dann_score` float NULL COMMENT "",
    `revel_score`float NULL COMMENT "",
    `lrt_score` float NULL COMMENT "",
    `lrt_pred` varchar(1) NULL COMMENT "",
    `phyloP17way_primate` float NULL COMMENT "",
    `phyloP100way_vertebrate` float NULL COMMENT "",
    `aa_change` varchar(1000) NULL COMMENT "",
    `dna_change` varchar(1000) NULL COMMENT ""
)
ENGINE=OLAP
PRIMARY KEY(`locus_id`, `symbol`, `transcript_id`)
DISTRIBUTED BY HASH(`locus_id`)
BUCKETS 10
PROPERTIES (
	"colocate_with" = "{{ params.colocate_query_group }}"
);
