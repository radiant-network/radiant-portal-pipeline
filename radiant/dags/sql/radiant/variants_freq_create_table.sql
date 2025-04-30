CREATE TABLE IF NOT EXISTS {{ params.starrocks_variants_frequencies }} (
    `locus_id` BIGINT NOT NULL,
    `pc` BIGINT,
    `ac` BIGINT,
    `hom` BIGINT
)
PRIMARY KEY(`locus_id`)
DISTRIBUTED BY HASH(`locus_id`)
BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ params.colocate_query_group }}"
)

