CREATE DATABASE IF NOT EXISTS ${output_db};

-- Pairs of related samples of the tenant (somalier relate over the whole cohort, relatedness >= 0.1), loaded
-- outside this run: a de novo is confirmed (PS2) only when both parents are parent-child with the proband.
CREATE TABLE IF NOT EXISTS ${output_db}.sample_relatedness (
    sample_a VARCHAR(255) NOT NULL,
    sample_b VARCHAR(255) NOT NULL,
    seq_id_a INT,
    seq_id_b INT,
    case_a INT,
    case_b INT,
    relatedness DOUBLE,
    ibs0 BIGINT,
    ibs2 BIGINT,
    n BIGINT
)
DUPLICATE KEY (sample_a, sample_b)
DISTRIBUTED BY HASH(sample_a) BUCKETS 1;

-- Criteria every ClinGen VCEP of the gene (current HGNC symbol) declares not applicable, rebuilt at each run from
-- the CSpec registry export on S3 (acmg.cspec): the classification drops them.
CREATE TABLE IF NOT EXISTS ${output_db}.cspec_not_applicable (
    symbol VARCHAR(100) NOT NULL,
    criterion VARCHAR(10) NOT NULL
)
PRIMARY KEY (symbol, criterion)
DISTRIBUTED BY HASH(symbol) BUCKETS 1;

-- ClinVar missense variants by MANE Select residue, rebuilt at each run from the file on S3 (acmg.protein): PS1, PM5.
CREATE TABLE IF NOT EXISTS ${output_db}.clinvar_protein (
    transcript_id VARCHAR(50) NOT NULL,
    position INT NOT NULL,
    variation_id VARCHAR(20) NOT NULL,
    ref_aa VARCHAR(3),
    alt_aa VARCHAR(3)
)
DUPLICATE KEY (transcript_id, position)
DISTRIBUTED BY HASH(transcript_id) BUCKETS 8;

-- Current HGNC symbol of each current or previous symbol, rebuilt at each run from HGNC on S3 (acmg.hgnc).
CREATE TABLE IF NOT EXISTS ${output_db}.gene_symbol (
    symbol VARCHAR(100) NOT NULL,
    current VARCHAR(100) NOT NULL
)
PRIMARY KEY (symbol)
DISTRIBUTED BY HASH(symbol) BUCKETS 1;

-- ClinGen facts per gene (current HGNC symbol), rebuilt at each run from the ClinGen downloads on S3 (acmg.clingen).
CREATE TABLE IF NOT EXISTS ${output_db}.clingen_gene (
    symbol VARCHAR(100) NOT NULL,
    hi_score INT NULL,
    no_disease BOOLEAN NOT NULL
)
PRIMARY KEY (symbol)
DISTRIBUTED BY HASH(symbol) BUCKETS 1;

-- ClinGen VCEP classifications by ClinVar variation (clinvar.name), rebuilt at each run from the erepo export on S3
-- (acmg.erepo). The criteria replace the computed ones of the variant: acmg_variant_criteria rows with source clingen_vcep.
CREATE TABLE IF NOT EXISTS ${output_db}.clingen_erepo (
    variation_id VARCHAR(20) NOT NULL,
    assertion VARCHAR(30),
    panel VARCHAR(200),
    disease VARCHAR(500),
    mondo_id VARCHAR(30),
    url VARCHAR(500),
    pmids ARRAY<BIGINT>,
    criteria ARRAY<STRUCT<criterion VARCHAR(10), code VARCHAR(30), strength VARCHAR(20), points INT>>
)
PRIMARY KEY (variation_id)
DISTRIBUTED BY HASH(variation_id) BUCKETS 1;

-- PubMed articles ClinVar cites per variation (clinvar.name), rebuilt at each run from var_citations.txt on S3.
CREATE TABLE IF NOT EXISTS ${output_db}.clinvar_citation (
    variation_id VARCHAR(20) NOT NULL,
    pmids ARRAY<BIGINT>
)
PRIMARY KEY (variation_id)
DISTRIBUTED BY HASH(variation_id) BUCKETS 1;

-- Criteria met by a variant, before exclusions and caps (applied when an occurrence is classified).
CREATE TABLE IF NOT EXISTS ${output_db}.acmg_variant_criteria (
    locus_id BIGINT NOT NULL,
    transcript_id VARCHAR(100) NOT NULL,
    criterion VARCHAR(10) NOT NULL,
    code VARCHAR(30) NOT NULL,
    strength VARCHAR(20) NOT NULL,
    points INT NOT NULL,
    value DOUBLE NULL,
    threshold DOUBLE NULL,
    source VARCHAR(30) NULL,
    details ARRAY<STRUCT<name VARCHAR(50), value DOUBLE>>,
    refs ARRAY<BIGINT>,
    ruleset_version VARCHAR(30) NOT NULL
)
PRIMARY KEY (locus_id, transcript_id, criterion)
DISTRIBUTED BY HASH(locus_id) BUCKETS 10;

-- Criteria met by a case's own evidence on a locus.
CREATE TABLE IF NOT EXISTS ${output_db}.acmg_occurrence_criteria (
    seq_id BIGINT NOT NULL,
    locus_id BIGINT NOT NULL,
    criterion VARCHAR(10) NOT NULL,
    code VARCHAR(30) NOT NULL,
    strength VARCHAR(20) NOT NULL,
    points INT NOT NULL,
    value DOUBLE NULL,
    threshold DOUBLE NULL,
    source VARCHAR(30) NULL,
    details ARRAY<STRUCT<name VARCHAR(50), value DOUBLE>>,
    refs ARRAY<BIGINT>,
    ruleset_version VARCHAR(30) NOT NULL
)
PRIMARY KEY (seq_id, locus_id, criterion)
DISTRIBUTED BY HASH(locus_id) BUCKETS 10;

-- PP4 of a gene for a case, from the case's HPO terms; genes sharing no informative term with the case are left out.
CREATE TABLE IF NOT EXISTS ${output_db}.acmg_phenotype (
    seq_id BIGINT NOT NULL,
    symbol VARCHAR(100) NOT NULL,
    n_terms INT NOT NULL,
    score DOUBLE NOT NULL,
    top DOUBLE NOT NULL,
    pp4_points INT NOT NULL,
    ruleset_version VARCHAR(30) NOT NULL
)
PRIMARY KEY (seq_id, symbol)
DISTRIBUTED BY HASH(seq_id) BUCKETS 4;

-- Priority of each classified occurrence: ACMG points + PP4 + inheritance fit, as a probability of pathogenicity.
-- Sort a case by it.
CREATE TABLE IF NOT EXISTS ${output_db}.acmg_priority (
    seq_id BIGINT NOT NULL,
    locus_id BIGINT NOT NULL,
    transcript_id VARCHAR(100) NOT NULL,
    pp4_points INT NOT NULL,
    inheritance_points INT NOT NULL,
    priority_points INT NOT NULL,
    priority_probability DOUBLE NOT NULL,
    -- ClinVar's PubMed citations of the variant: for the reader, never in the points.
    pmid_count INT NOT NULL,
    published BOOLEAN NOT NULL,
    ruleset_version VARCHAR(30) NOT NULL
)
PRIMARY KEY (seq_id, locus_id, transcript_id)
DISTRIBUTED BY HASH(locus_id) BUCKETS 10;

-- ACMG class of each occurrence of a case on a classified (locus, transcript).
CREATE TABLE IF NOT EXISTS ${output_db}.acmg_classification (
    seq_id BIGINT NOT NULL,
    locus_id BIGINT NOT NULL,
    transcript_id VARCHAR(100) NOT NULL,
    acmg_class VARCHAR(30) NOT NULL,
    acmg_points INT NOT NULL,
    acmg_criteria ARRAY<VARCHAR(30)>,
    acmg_cancelled ARRAY<STRUCT<criterion VARCHAR(10), cancelled_by VARCHAR(10)>>,
    ruleset_version VARCHAR(30) NOT NULL
)
PRIMARY KEY (seq_id, locus_id, transcript_id)
DISTRIBUTED BY HASH(locus_id) BUCKETS 10;
