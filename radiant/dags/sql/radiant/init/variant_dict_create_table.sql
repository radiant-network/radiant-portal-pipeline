CREATE TABLE IF NOT EXISTS {{ params.starrocks_variants_lookup }} (
    locus_hash VARCHAR(64) NOT NULL,
    locus_id BIGINT NOT NULL AUTO_INCREMENT
)
ENGINE=OLAP
PRIMARY KEY (locus_hash)
DISTRIBUTED BY HASH(`locus_hash`) BUCKETS 5;
