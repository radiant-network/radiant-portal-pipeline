CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_snv_consequence }} (
    `locus_id` bigint(20) COMMENT "",
    `symbol` varchar(30) COMMENT "",
    `transcript_id` varchar(100) COMMENT "",
    `source` varchar(20) NULL COMMENT "",
    `consequences` array<varchar(50)> COMMENT "",
    `impact_score` tinyint(4) NULL COMMENT "",
    `biotype` varchar(50) NULL COMMENT "",
    `exon_rank` VARCHAR(10) NULL COMMENT "",
    `exon_total` VARCHAR(10) NULL COMMENT "",
    `spliceai_ds` float NULL COMMENT "",
    `spliceai_type` array<varchar(2)> NULL COMMENT "",
    `is_canonical` boolean NULL COMMENT "",
    `is_picked` boolean NULL COMMENT "",
    `is_mane_select` boolean NULL COMMENT "",
    `is_mane_plus` boolean NULL COMMENT "",
    `mane_select` varchar(200) NULL COMMENT "",
    `mane_pair_transcript_id` varchar(100) NULL COMMENT "",
    `sift_score` float NULL COMMENT "",
    `sift_pred` varchar(1) NULL COMMENT "",
    `polyphen2_hvar_score` float NULL COMMENT "",
    `polyphen2_hvar_pred` varchar(1) NULL COMMENT "",
    `fathmm_score` float NULL COMMENT "",
    `fathmm_pred` varchar(1) NULL COMMENT "",
    `cadd_score` float NULL COMMENT "",
    `cadd_phred` float NULL COMMENT "",
    `dann_score` float NULL COMMENT "",
    `revel_score`float NULL COMMENT "",
    `lrt_score` float NULL COMMENT "",
    `lrt_pred` varchar(1) NULL COMMENT "",
    `gnomad_pli` float NULL COMMENT "",
    `gnomad_loeuf` float NULL COMMENT "",
    `phyloP17way_primate` float NULL COMMENT "",
    `phyloP100way_vertebrate` float NULL COMMENT "",
    -- SJRA-1827. True when the score columns above were looked up through this row's MANE twin in the
    -- other catalogue instead of its own transcript. Only a RefSeq MANE Select row can be true:
    -- dbNSFP and gnomAD constraint are both keyed on Ensembl transcript ids.
    --
    -- It states the provenance of the *join key*, not that values were found -- a true flag with a null
    -- `sift_score` means dbNSFP does not cover that locus, exactly as on an Ensembl row. Consumers must
    -- still null-check each value and render "not available" rather than reading the flag as "scored".
    --
    -- NOT NULL DEFAULT "false" so the rows loaded before SJRA-1827 read false without a rewrite: they
    -- are all Ensembl, for which false is factual. Declared identically in
    -- migrations/SJRA-1820_snv_consequences_add_columns.sql -- the two MUST agree, or a fresh
    -- deployment and a migrated one disagree on nullability and on the introspecting dbt sweeps.
    `scores_from_mane_pair` boolean NOT NULL DEFAULT "false" COMMENT "",
    `vep_impact` VARCHAR(20) NULL COMMENT "",
    `aa_change` varchar(1000) NULL COMMENT "",
    `dna_change` varchar(1000) NULL COMMENT ""
)
ENGINE=OLAP
PRIMARY KEY(`locus_id`, `symbol`, `transcript_id`)
DISTRIBUTED BY HASH(`locus_id`)
BUCKETS 10
PROPERTIES (
	"colocate_with" = "{{ mapping.colocate_query_group }}"
);
