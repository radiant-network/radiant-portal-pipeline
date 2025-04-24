CREATE TABLE IF NOT EXISTS 1000_genomes (
    locus_id BIGINT NOT NULL,
    af DECIMAL(7, 6)
)
ENGINE=OLAP
DISTRIBUTED BY HASH(locus_id) BUCKETS 10
PROPERTIES (
    "colocate_with" = "query_group"
);