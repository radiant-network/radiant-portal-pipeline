CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_somatic_snv_occurrence }} (
    part             INT NOT NULL,
    task_id          INT NOT NULL,
    tumor_seq_id     INT NOT NULL,
    locus_id         BIGINT NOT NULL,
    normal_seq_id    INT,
    quality          FLOAT,
    filter           VARCHAR(255),
    -- INFO fields
    info_hotspotallele              VARCHAR(255),
    info_old_record                 VARCHAR(2000),
    info_baseq_rank_sum             FLOAT,
    info_excess_het                 FLOAT,
    info_fs                         FLOAT,
    info_ds                         BOOLEAN,
    info_fraction_informative_reads FLOAT,
    info_inbreed_coeff              FLOAT,
    info_mleac                      INT,
    info_mleaf                      FLOAT,
    info_mq                         FLOAT,
    info_mq0                        FLOAT,
    info_m_qrank_sum                FLOAT,
    info_qd                         FLOAT,
    info_r2_5p_bias                 FLOAT,
    info_read_pos_rank_sum          FLOAT,
    info_sor                        FLOAT,
    info_vqslod                     FLOAT,
    info_culprit                    VARCHAR(255),
    info_dp                         INT,
    info_haplotype_score            FLOAT,
    -- Tumor FORMAT
    tumor_calls      ARRAY<INT>,
    tumor_dp         INT,
    tumor_gq         INT,
    tumor_has_alt    BOOLEAN,
    tumor_af         FLOAT,
    tumor_zygosity   CHAR(3),
    tumor_ad_ref     INT,
    tumor_ad_alt     INT,
    tumor_ad_total   INT,
    tumor_ad_ratio   FLOAT,
    tumor_phased     BOOLEAN,
    tumor_gt_status  VARCHAR(50),
    -- Normal FORMAT
    normal_calls     ARRAY<INT>,
    normal_dp        INT,
    normal_gq        INT,
    normal_has_alt   BOOLEAN,
    normal_af        FLOAT,
    normal_zygosity  CHAR(3),
    normal_ad_ref    INT,
    normal_ad_alt    INT,
    normal_ad_total  INT,
    normal_ad_ratio  FLOAT,
    normal_phased    BOOLEAN,
    normal_gt_status VARCHAR(50),
    INDEX locus_id_index (`locus_id`) USING BITMAP COMMENT ''
)
ENGINE=OLAP
DUPLICATE KEY(`part`, `task_id`, `tumor_seq_id`, `locus_id`)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ mapping.colocate_query_group }}"
);