CREATE TABLE IF NOT EXISTS kf_variants (
    locus_id BIGINT NOT NULL,
    af DECIMAL(7, 6),
    pf DECIMAL(7, 6),
    gnomad_v3_af DECIMAL(7, 6),
    topmed_af DECIMAL(7, 6),
    tg_af DECIMAL(7, 6),
    ac INT(11),
    pc INT(11),
    hom INT(11),
    chromosome CHAR(2),
    start BIGINT NULL COMMENT '',
    variant_class VARCHAR(50) NULL COMMENT '',
    clinvar_interpretation ARRAY<VARCHAR(100)> NULL COMMENT '',
    symbol VARCHAR(20) NULL COMMENT '',
    consequence ARRAY<VARCHAR(50)> NULL COMMENT '',
    vep_impact VARCHAR(20) NULL COMMENT '',
    mane_select BOOLEAN NULL COMMENT '',
    mane_plus BOOLEAN NULL COMMENT '',
    picked BOOLEAN NULL COMMENT '',
    canonical BOOLEAN NULL COMMENT '',
    rsnumber ARRAY<VARCHAR(15)> NULL COMMENT '',
    reference VARCHAR(2000),
    alternate VARCHAR(2000),
    hgvsg VARCHAR(2000) NULL,
    locus VARCHAR(2000) NULL,
    dna_change VARCHAR(2000),
    aa_change VARCHAR(2000)
)
DISTRIBUTED BY HASH(locus_id) BUCKETS 10
PROPERTIES (
    'colocate_with' = 'query_group'
);