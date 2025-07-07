CREATE TABLE IF NOT EXISTS {{ params.starrocks_staging_exomiser }}
(
    part                 INT,
    seq_id               INT,
    id                   VARCHAR(2000),
    rank                 INT,
    symbol               VARCHAR(200),
    entrez_gene_id       VARCHAR(200),
    moi                  VARCHAR(10),
    variant_score        FLOAT,
    gene_combined_score  FLOAT,
    contributing_variant BOOLEAN,
    chromosome           VARCHAR(10),
    start                INT,
    end                  INT,
    reference            VARCHAR(2000),
    alternate            VARCHAR(2000),
    acmg_interpretation  VARCHAR(300),
    acmg_evidence        ARRAY<VARCHAR (10)>,
    locus                VARCHAR(2000) AS (concat_ws('-', chromosome, start, reference, alternate)),
    locus_hash           VARCHAR(256) AS (sha2(concat_ws('-', chromosome, start, reference, alternate), 256))
)
ENGINE = OLAP DUPLICATE KEY(`part`, `seq_id`, `id`)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`locus_hash`) BUCKETS 10;