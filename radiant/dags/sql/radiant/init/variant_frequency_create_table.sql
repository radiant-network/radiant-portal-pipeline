CREATE TABLE IF NOT EXISTS {{ params.starrocks_variant_frequency }} (
   `locus_id` BIGINT NOT NULL,
   `pc` BIGINT,
   `pn` BIGINT,
   `pf` DOUBLE
)
DISTRIBUTED BY HASH(`locus_id`)
BUCKETS 10
PROPERTIES (
   "colocate_with" = "{{ params.colocate_query_group }}"
)
