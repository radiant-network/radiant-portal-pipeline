-- COSMIC Mutation Census mutations that coordinates cannot place: no GRCh38 position in the export, or
-- alleles the reference contradicts (COSMIC's liftover kept GRCh37-strand alleles in inverted segments).
-- Keyed on the HGVS coding change instead, one row per (transcript, c. change); the transcript is the
-- Ensembl accession without its version. Complements cosmic_mutation_set, never overlaps its loci.
CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_cosmic_mutation_set_hgvs }}
(
    `transcript_id`  VARCHAR(32)   NOT NULL,
    `cds_change`     VARCHAR(2000) NOT NULL,
    `mutation_url`   VARCHAR(255)  NULL,
    `shared_aa`      INT           NULL,
    `cosmic_id`      VARCHAR(32)   NULL,
    `sample_mutated` INT           NULL,
    `sample_tested`  INT           NULL,
    `tier`           VARCHAR(8)    NULL,
    `sample_ratio`   DOUBLE        NULL
)
ENGINE = OLAP
DUPLICATE KEY(`transcript_id`, `cds_change`)
DISTRIBUTED BY HASH(`transcript_id`, `cds_change`)
BUCKETS 10;
