CREATE TABLE IF NOT EXISTS {{ params.starrocks_consequences_filter }} (
    `locus_id` bigint(20) NULL COMMENT "",
    `is_deleterious` boolean NOT NULL COMMENT "",
    `impact_score` tinyint(4) NULL COMMENT "",
    `symbol` varchar(30) NULL COMMENT "",
    `consequence` varchar(50) NULL COMMENT "",
    `biotype` varchar(50) NULL COMMENT "",
    `spliceai_ds` float NULL COMMENT "",
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
    `gnomad_pli` float NULL COMMENT "",
    `gnomad_loeuf` float NULL COMMENT "",
    `phyloP17way_primate` float NULL COMMENT "",
    `phyloP100way_vertebrate` float NULL COMMENT ""
)
ENGINE=OLAP
DUPLICATE KEY(`locus_id`, `is_deleterious`, `impact_score`)
COMMENT "OLAP"
PARTITION BY (`is_deleterious`)
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES ("colocate_with" = "{{ params.colocate_query_group }}");
