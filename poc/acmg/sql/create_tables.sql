CREATE DATABASE IF NOT EXISTS ${output_db};

-- ClinGen facts per gene, rebuilt at each run from the ClinGen downloads on S3 (acmg.clingen).
CREATE TABLE IF NOT EXISTS ${output_db}.clingen_gene (
    symbol VARCHAR(100) NOT NULL,
    hi_score INT NULL,
    no_disease BOOLEAN NOT NULL
)
PRIMARY KEY (symbol)
DISTRIBUTED BY HASH(symbol) BUCKETS 1;

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

-- Priority of each classified occurrence: ACMG points + PP4, as a probability of pathogenicity. Sort a case by it.
CREATE TABLE IF NOT EXISTS ${output_db}.acmg_priority (
    seq_id BIGINT NOT NULL,
    locus_id BIGINT NOT NULL,
    transcript_id VARCHAR(100) NOT NULL,
    pp4_points INT NOT NULL,
    priority_points INT NOT NULL,
    priority_probability DOUBLE NOT NULL,
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
    ruleset_version VARCHAR(30) NOT NULL
)
PRIMARY KEY (seq_id, locus_id, transcript_id)
DISTRIBUTED BY HASH(locus_id) BUCKETS 10;
