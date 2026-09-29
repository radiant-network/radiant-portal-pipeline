-- Loads the *normalized* COSMIC Mutation Census (the gzipped TSV written by the normalize task of
-- radiant-import-cosmic-mutation-set, not the cmc_export.tsv.gz download) into raw_cosmic_mutation_set.
--
-- The placeholder list is positional and follows OUTPUT_COLUMNS in radiant/tasks/open_data/cosmic_mutation_set.py:
--   chromosome, start, reference, alternate, locus_hash, mutation_url, shared_aa, cosmic_id,
--   sample_mutated, sample_tested, tier
-- Integer cells can be empty (SHARED_AA is blank for many rows); tier is kept as published ('1','2','3','Other').
LOAD LABEL {{ database_name }}.{{ load_label }}
(
    DATA INFILE %(tsv_filepath)s
    INTO TABLE {{ table_name }}
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "CSV"
    (
        skip_header = 1
    )
    (chromosome, start, reference, alternate, locus_hash, mutation_url, temp_shared_aa, cosmic_id,
     temp_sample_mutated, temp_sample_tested, temp_tier)
    SET
    (
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
