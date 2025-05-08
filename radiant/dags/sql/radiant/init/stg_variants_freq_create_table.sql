CREATE TABLE IF NOT EXISTS {{ params.starrocks_staging_variants_frequencies }} (
    `part` INT NOT NULL,
    `locus_id` BIGINT NOT NULL,
    `pc` BIGINT,
    `pn` BIGINT
)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`locus_id`)
BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ params.colocate_query_group }}"
)

