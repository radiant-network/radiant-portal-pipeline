-- Loads the *normalized* COSMIC Mutation Census (the gzipped TSV written by the normalize task of
-- radiant-import-cosmic-mutation-set, not the cmc_export.tsv.gz download) into raw_cosmic_mutation_set.
--
-- The placeholder list is positional and follows OUTPUT_COLUMNS in radiant/tasks/open_data/cosmic_mutation_set.py:
--   chromosome, start, reference, alternate, locus_hash, mutation_url, shared_aa, cosmic_id,
--   sample_mutated, sample_tested, tier, transcript_id, cds_change
-- A row the normalizer could not key (no GRCh38 position, reference allele contradicted by the FASTA, ...)
-- arrives with its five key cells empty and is stored with them NULL: cosmic_mutation_set skips it, and
-- cosmic_mutation_set_hgvs is built from exactly those rows. Integer cells can be empty (SHARED_AA is blank
-- for many rows); tier is kept as published ('1','2','3','Other').
LOAD LABEL {{ database_name }}.{{ load_label }}
(
    DATA INFILE %(tsv_filepath)s
    INTO TABLE {{ table_name }}
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "CSV"
    (
        skip_header = 1
    )
    (temp_chromosome, temp_start, temp_reference, temp_alternate, temp_locus_hash, mutation_url, temp_shared_aa,
     cosmic_id, temp_sample_mutated, temp_sample_tested, temp_tier, transcript_id, cds_change)
    SET
    (
        chromosome = nullif(temp_chromosome, ''),
        start = nullif(temp_start, ''),
        reference = nullif(temp_reference, ''),
        alternate = nullif(temp_alternate, ''),
        locus_hash = nullif(temp_locus_hash, ''),
        mutation_url = nullif(mutation_url, ''),
        shared_aa = nullif(temp_shared_aa, ''),
        cosmic_id = nullif(cosmic_id, ''),
        sample_mutated = nullif(temp_sample_mutated, ''),
        sample_tested = nullif(temp_sample_tested, ''),
        tier = nullif(temp_tier, '')
    )
)
WITH BROKER
(
    {{ broker_configuration }}
)
PROPERTIES
(
    'timeout' = '{{ broker_load_timeout }}'
);
