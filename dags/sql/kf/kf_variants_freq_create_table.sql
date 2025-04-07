CREATE TABLE IF NOT EXISTS kf_variants_freq (
    `locus_id` BIGINT NOT NULL,
    `pc` BIGINT,
    `ac` BIGINT,
    `hom` BIGINT
)
PRIMARY KEY(`locus_id`)
DISTRIBUTED BY HASH(`locus_id`)
BUCKETS 10
PROPERTIES (
    "colocate_with" = "query_group"
)

