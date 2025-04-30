CREATE TABLE IF NOT EXISTS {{ params.starrocks_occurrences }} (
  `part` tinyint(4) NOT NULL COMMENT "",
  `seq_id` int(11) NOT NULL COMMENT "",
  `locus_id` bigint(20) NOT NULL COMMENT "",
  `ad_ratio` decimal(6, 5) NULL COMMENT "",
  `ad_total` int(11) NULL COMMENT "",
  `ad_ref` int(11) NULL COMMENT "",
  `ad_alt` int(11) NULL COMMENT "",
  `chromosome` varchar(2) NULL COMMENT "",
  `start` bigint(20) NULL COMMENT "",
  `zygosity` varchar(5) NULL COMMENT "",
  `has_alt` boolean NULL COMMENT "",
  `filter` varchar(100) NULL COMMENT "",
  `info_ac` int(11) NULL COMMENT "",
  `info_an` int(11) NULL COMMENT "",
  `info_af` decimal(6, 5) NULL COMMENT "",
  `info_baseq_rank_sum` decimal(10, 4) NULL COMMENT "",
  `info_excess_het` decimal(10, 4) NULL COMMENT "",
  `info_ds` boolean NULL COMMENT "",
  `info_inbreed_coeff` decimal(6, 4) NULL COMMENT "",
  `info_mleac` int(11) NULL COMMENT "",
  `info_mleaf` decimal(6, 4) NULL COMMENT "",
  `info_mq` decimal(7, 3) NULL COMMENT "",
  `info_m_qrank_sum` decimal(10, 4) NULL COMMENT "",
  `info_qd` decimal(7, 3) NULL COMMENT "",
  `info_read_pos_rank_sum` decimal(7, 3) NULL COMMENT "",
  `info_vqslod` decimal(7, 3) NULL COMMENT "",
  `info_culprit` varchar(20) NULL COMMENT "",
  `info_dp` int(11) NULL COMMENT "",
  `info_haplotype_score` decimal(6, 4) NULL COMMENT "",
  `calls` array<int(11)> NULL COMMENT "",
  INDEX locus_id_index (`locus_id`) USING BITMAP COMMENT ''
)
ENGINE=OLAP
DUPLICATE KEY(`part`, `seq_id`, `locus_id`)
COMMENT "OLAP"
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ params.colocate_query_group }}"
);