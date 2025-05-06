CREATE TABLE IF NOT EXISTS {{ params.starrocks_1000_genomes }} (
    locus_id BIGINT NOT NULL,
    af DOUBLE
)
ENGINE=OLAP
DISTRIBUTED BY HASH(locus_id) BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ params.colocate_query_group }}"
);