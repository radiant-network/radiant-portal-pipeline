CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_1000_genomes }} (
    locus_id BIGINT NOT NULL,
    af DOUBLE,
    ac INT(11),
    an INT(11)
)
ENGINE=OLAP
DISTRIBUTED BY HASH(locus_id) BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ mapping.colocate_query_group }}"
);