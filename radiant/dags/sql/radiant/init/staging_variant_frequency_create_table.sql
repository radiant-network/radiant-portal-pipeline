CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_staging_variant_frequency }} (
    `part` INT NOT NULL,
    `locus_id` BIGINT NOT NULL,
    `pc_wgs` BIGINT,
    `pn_wgs` BIGINT,
    `pc_wgs_affected` BIGINT,
    `pn_wgs_affected` BIGINT,
    `pc_wgs_not_affected` BIGINT,
    `pn_wgs_not_affected` BIGINT,
    `pc_wxs` BIGINT,
    `pn_wxs` BIGINT,
    `pc_wxs_affected` BIGINT,
    `pn_wxs_affected` BIGINT,
    `pc_wxs_not_affected` BIGINT,
    `pn_wxs_not_affected` BIGINT
)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`locus_id`)
BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ mapping.colocate_query_group }}"
)

