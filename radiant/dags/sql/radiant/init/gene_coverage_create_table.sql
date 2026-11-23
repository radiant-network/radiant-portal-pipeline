-- RAD-315. The per-gene coverage of each current alignment (`alignment_germline_variant_calling` or
-- `alignment_somatic_variant_calling`), CDS genes only. One row per (seq_id, task_id, symbol, chromosome): `symbol` is
-- the resolved symbol (else the ENSG id), `chromosome` comes from the `_chrX` / `_chrY` suffix, else from
-- `ensembl_gene`, `gene` keeps the raw name of the file. `part` is for the partition swap of import_part.
CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_gene_coverage }}
(
    part             INT,
    seq_id           INT,
    task_id          INT,
    symbol           VARCHAR(200),
    chromosome       VARCHAR(10),
    gene             VARCHAR(200),
    ensembl_gene_id  VARCHAR(128),
    size             INT,
    average_coverage DOUBLE,
    coverage_5x      FLOAT,
    coverage_15x     FLOAT,
    coverage_30x     FLOAT,
    coverage_50x     FLOAT,
    coverage_100x    FLOAT,
    coverage_200x    FLOAT,
    coverage_300x    FLOAT,
    coverage_400x    FLOAT,
    coverage_500x    FLOAT,
    coverage_1000x   FLOAT
)
ENGINE = OLAP
DUPLICATE KEY(`part`, `seq_id`, `task_id`, `symbol`, `chromosome`)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`seq_id`)
BUCKETS 10;
