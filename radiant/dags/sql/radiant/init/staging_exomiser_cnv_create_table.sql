{#- The CNV Exomiser reports (`exomiser_cnv`, RAD-181) of the `radiant_germline_cnv_annotation` tasks, apart from the
    SNV ones (`raw_exomiser`): the two reports can change format apart, and no CNV row sits where the SNV Exomiser
    insert reads. Same ordinary columns as `raw_exomiser`, plus `vcf_id` (the TSV's `VCF_ID`), the key the tenant
    `exomiser_cnv` insert joins on (to `name`, else to the occurrence's `vcf_id`). No `locus` / `locus_hash`: a CNV is
    matched on its call ID, not on its locus. #}
CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_staging_exomiser_cnv }}
(
    part                 INT,
    seq_id               INT,
    task_id              INT,
    vcf_id               VARCHAR(2000),
    id                   VARCHAR(2000),
    tenant_code          VARCHAR(50),
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
    acmg_classification  VARCHAR(300),
    acmg_evidence        ARRAY<VARCHAR (10)>
)
ENGINE = OLAP DUPLICATE KEY(`part`, `seq_id`, `task_id`, `vcf_id`)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`seq_id`) BUCKETS 10;
