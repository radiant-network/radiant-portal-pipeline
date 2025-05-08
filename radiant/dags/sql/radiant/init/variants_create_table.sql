CREATE TABLE IF NOT EXISTS {{ params.starrocks_variants }} (
    locus_id BIGINT NOT NULL,
    pf DOUBLE,
    gnomad_v3_af DOUBLE,
    topmed_af DOUBLE,
    tg_af DOUBLE,
    pc INT(11),
    pn INT(11),
    chromosome CHAR(2),
    start BIGINT NULL COMMENT '',
    clinvar_name VARCHAR(2000) NULL COMMENT '',
    variant_class VARCHAR(50) NULL COMMENT '',
    clinvar_interpretation ARRAY<VARCHAR(100)> NULL COMMENT '',
    symbol VARCHAR(20) NULL COMMENT '',
    impact_score tinyint NULL COMMENT "",
    consequences ARRAY<VARCHAR(50)> NULL COMMENT '',
    vep_impact VARCHAR(20) NULL COMMENT '',
    is_mane_select BOOLEAN NULL COMMENT '',
    is_mane_plus BOOLEAN NULL COMMENT '',
    is_canonical BOOLEAN NULL COMMENT '',
    rsnumber ARRAY<VARCHAR(15)> NULL COMMENT '',
    reference VARCHAR(2000),
    alternate VARCHAR(2000),
    mane_select varchar(200) NULL,
    hgvsg VARCHAR(2000) NULL,
    hgvsc varchar(2000) NULL,
    hgvsp varchar(2000) NULL,
    locus VARCHAR(2000) NULL,
    dna_change VARCHAR(2000),
    aa_change VARCHAR(2000)
)
DISTRIBUTED BY HASH(locus_id) BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ params.colocate_query_group }}"
);