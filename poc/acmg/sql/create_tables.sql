CREATE DATABASE IF NOT EXISTS ${output_db};

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
