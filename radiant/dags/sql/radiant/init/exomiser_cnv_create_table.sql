{#- The CNV Exomiser rows of each germline CNV occurrence (RAD-181), per tenant: the twin of `exomiser` for SNVs.
    Every contributing row of the report (one per gene and mode of inheritance), not only the best one.
    `cnv_id` and `name` stand where `exomiser` has `locus_id` and `locus_hash`: they are the occurrence's own keys, so
    a row joins `germline__cnv__occurrence` on (part, seq_id, task_id, name) or on `cnv_id`. `vcf_id` keeps the
    report's `VCF_ID`, which is `name` or the slivar record's ID (see exomiser_cnv_insert_partition_delta.sql).
    `variant_rank` = 1 is the row the occurrence copies, as for SNVs. #}
CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_exomiser_cnv }}
(
    part                 INT,
    seq_id               INT,
    task_id              INT NOT NULL,
    cnv_id               BIGINT,
    id                   VARCHAR(2000),
    name                 VARCHAR(1048576),
    vcf_id               VARCHAR(2000),
    moi                  VARCHAR(10),
    variant_score        FLOAT,
    gene_combined_score  FLOAT,
    variant_rank         INT,
    rank                 INT,
    symbol               VARCHAR(200),
    acmg_classification  VARCHAR(300),
    acmg_evidence        ARRAY<VARCHAR(10)>
)
ENGINE = OLAP
DUPLICATE KEY(`part`, `seq_id`, `task_id`, `cnv_id`, `id`)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`seq_id`) BUCKETS 10;
