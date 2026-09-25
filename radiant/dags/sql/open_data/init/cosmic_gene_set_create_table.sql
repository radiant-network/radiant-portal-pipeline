-- COSMIC Cancer Gene Census, one row per gene, with the columns the legacy Spark ETL produced
-- (datalake-lib CosmicGeneSet.scala). Loaded from the census TSV by radiant-import-cosmic-gene-set.
CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_cosmic_gene_set }}
(
    `symbol`                  VARCHAR(30)          NOT NULL,
    `chromosome`              VARCHAR(10)          NULL,
    `start`                   BIGINT               NULL,
    `name`                    VARCHAR(255)         NULL,
    `cosmic_gene_id`          VARCHAR(30)          NULL,
    `tier`                    INT                  NULL,
    `chr_band`                VARCHAR(30)          NULL,
    `somatic`                 BOOLEAN              NOT NULL,
    `germline`                BOOLEAN              NOT NULL,
    `tumour_types_somatic`    ARRAY<VARCHAR(255)>  NULL,
    `tumour_types_germline`   ARRAY<VARCHAR(255)>  NULL,
    `cancer_syndrome`         VARCHAR(255)         NULL,
    `tissue_type`             ARRAY<VARCHAR(255)>  NULL,
    `molecular_genetics`      VARCHAR(50)          NULL,
    `role_in_cancer`          ARRAY<VARCHAR(255)>  NULL,
    `mutation_types`          ARRAY<VARCHAR(255)>  NULL,
    `translocation_partner`   ARRAY<VARCHAR(255)>  NULL,
    `other_germline_mutation` BOOLEAN              NOT NULL,
    `other_syndrome`          ARRAY<VARCHAR(255)>  NULL,
    `synonyms`                ARRAY<VARCHAR(255)>  NULL
)
ENGINE = OLAP
DUPLICATE KEY(`symbol`);
