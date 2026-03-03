CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_germline_snv_variant_frequency }} (
   `locus_id` BIGINT NOT NULL,
   `pc_wgs` BIGINT,
   `pn_wgs` BIGINT,
   `pf_wgs` DOUBLE,
   `pc_wgs_affected` BIGINT,
   `pn_wgs_affected` BIGINT,
   `pf_wgs_affected` DOUBLE,
   `pc_wgs_not_affected` BIGINT,
   `pn_wgs_not_affected` BIGINT,
   `pf_wgs_not_affected` DOUBLE,
   `pc_wxs` BIGINT,
   `pn_wxs` BIGINT,
   `pf_wxs` DOUBLE,
   `pc_wxs_affected` BIGINT,
   `pn_wxs_affected` BIGINT,
   `pf_wxs_affected` DOUBLE,
   `pc_wxs_not_affected` BIGINT,
   `pn_wxs_not_affected` BIGINT,
   `pf_wxs_not_affected` DOUBLE
)
DISTRIBUTED BY HASH(`locus_id`)
BUCKETS 10
PROPERTIES (
   "colocate_with" = "{{ mapping.colocate_query_group }}"
)
