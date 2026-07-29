CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_somatic_snv_staging_variant_frequency }} (
    `part` INT NOT NULL,
    `locus_id` BIGINT NOT NULL,
	`pc_tn_wgs` BIGINT,
	`pn_tn_wgs` BIGINT,
	`pf_tn_wgs` DOUBLE,
	`pc_tn_wxs` BIGINT,
	`pn_tn_wxs` BIGINT,
	`pf_tn_wxs` DOUBLE,
	`pc_to_wgs` BIGINT,
	`pn_to_wgs` BIGINT,
	`pf_to_wgs` DOUBLE,
	`pc_to_wxs` BIGINT,
	`pn_to_wxs` BIGINT,
	`pf_to_wxs` DOUBLE
)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`locus_id`)
BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ mapping.colocate_query_group }}"
)

