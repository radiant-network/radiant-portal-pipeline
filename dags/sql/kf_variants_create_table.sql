create table if not exists kf_variants
(
    locus_id BIGINT NOT NULL,
    `af`                     decimal(7, 6),
    `pf`                     decimal(7, 6),
    `gnomad_v3_af`           decimal(7, 6),
    `topmed_af`           decimal(7, 6),
    `tg_af`           decimal(7, 6),
    `ac`                     int(11),
    `pc`                     int(11),
    `hom`                    int(11),
    `chromosome`             char(2),
    `start`                  bigint NULL COMMENT "",
    `variant_class`          varchar(50) NULL COMMENT "",
    `clinvar_interpretation` array< varchar (100)> NULL COMMENT "",
    `symbol`                 varchar(20) NULL COMMENT "",
    `consequence`            array< varchar (50)> NULL COMMENT "",
    `vep_impact`             varchar(20) NULL COMMENT "",
    `mane_select`            boolean NULL COMMENT "",
    `mane_plus`            boolean NULL COMMENT "",
    `picked`            boolean NULL COMMENT "",
    `canonical`              boolean NULL COMMENT "",
    `rsnumber`               array< varchar (15)> NULL COMMENT "",
    `reference`              varchar(2000),
    `alternate`              varchar(2000),
    `hgvsg`                  varchar(2000) NULL,
    `locus`             varchar(2000) NULL,
    `dna_change`             varchar(2000),
    `aa_change`             varchar(2000)
)
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES (
"colocate_with" = "query_group"
);