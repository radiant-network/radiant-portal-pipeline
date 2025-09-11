CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_exomiser }}
(
    part                 INT,
    seq_id               INT,
    locus_id             BIGINT,
    id                   VARCHAR(2000),
    locus_hash           VARCHAR(256),
    moi                  VARCHAR(10),
    variant_score        FLOAT,
    gene_combined_score  FLOAT,
    variant_rank         TINYINT,
    rank                 INT,
    symbol               VARCHAR(200),
    acmg_classification  VARCHAR(300),
    acmg_evidence array< VARCHAR (10)>
)
ENGINE = OLAP
DUPLICATE KEY(`part`, `seq_id`, `locus_id`,  `id`)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`locus_id`)
BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ mapping.colocate_query_group }}"
);