CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_somatic_cnv_variant_frequency }} (
   `cnv_id` BIGINT NOT NULL,
   `pc_to_wgs` BIGINT,
   `pn_to_wgs` BIGINT,
   `pf_to_wgs` DOUBLE,
   `pc_to_wxs` BIGINT,
   `pn_to_wxs` BIGINT,
   `pf_to_wxs` DOUBLE
)
DISTRIBUTED BY HASH(`cnv_id`)
BUCKETS 10
