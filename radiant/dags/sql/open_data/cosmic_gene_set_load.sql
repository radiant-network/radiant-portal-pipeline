-- Loads the COSMIC Cancer Gene Census TSV (Cosmic_CancerGeneCensus_GRCh38.tsv.gz) into cosmic_gene_set.
--
-- The placeholder list is positional and follows the census header:
--   GENE_SYMBOL, NAME, COSMIC_GENE_ID, CHROMOSOME, GENOME_START, GENOME_STOP, CHR_BAND, SOMATIC, GERMLINE,
--   TUMOUR_TYPES_SOMATIC, TUMOUR_TYPES_GERMLINE, CANCER_SYNDROME, TISSUE_TYPE, MOLECULAR_GENETICS,
--   ROLE_IN_CANCER, MUTATION_TYPES, TRANSLOCATION_PARTNER, OTHER_GERMLINE_MUT, OTHER_SYNDROME, TIER, SYNONYMS
-- (21 columns, verified against the GRCh38 census download). `temp_*` placeholders are read but not
-- stored (GENOME_STOP was dropped by the legacy ETL too).
--
-- The SET clause reproduces the legacy Spark transform: `y` flags become booleans, comma-separated
-- lists become arrays with each element trimmed, and an empty cell becomes NULL (Spark read it as
-- null, so `split` yielded null rather than `[""]`).
--
-- The census download has CRLF line endings. StarRocks splits rows on LF, so the last column of every
-- row (SYNONYMS) arrives with a trailing CR; `list_of` strips leading/trailing whitespace before
-- anything else, so it does not matter which list column is last. Keep a list column last.
{%- macro list_of(col) -%}
IF(regexp_replace({{ col }}, '^\\s+|\\s+$', '') = '', NULL,
   split(regexp_replace(regexp_replace({{ col }}, '^\\s+|\\s+$', ''), '\\s*,\\s*', ','), ','))
{%- endmacro %}
LOAD LABEL {{ database_name }}.{{ load_label }}
(
    DATA INFILE %(tsv_filepath)s
    INTO TABLE {{ table_name }}
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "CSV"
    (
        skip_header = 1
        enclose = "\""
    )
    (gene_symbol, name, cosmic_gene_id, chromosome, genome_start, temp_genome_stop, chr_band, temp_somatic,
     temp_germline, temp_tumour_types_somatic, temp_tumour_types_germline, cancer_syndrome, temp_tissue_type,
     molecular_genetics, temp_role_in_cancer, temp_mutation_types, temp_translocation_partner,
     temp_other_germline_mut, temp_other_syndrome, tier, temp_synonyms)
    SET
    (
        symbol = gene_symbol,
        chromosome = nullif(chromosome, ''),
        start = nullif(genome_start, ''),
        name = nullif(name, ''),
        cosmic_gene_id = nullif(cosmic_gene_id, ''),
        tier = nullif(tier, ''),
        chr_band = nullif(chr_band, ''),
        somatic = IF(temp_somatic = 'y', 1, 0),
        germline = IF(temp_germline = 'y', 1, 0),
        tumour_types_somatic = {{ list_of('temp_tumour_types_somatic') }},
        tumour_types_germline = {{ list_of('temp_tumour_types_germline') }},
        cancer_syndrome = nullif(cancer_syndrome, ''),
        tissue_type = {{ list_of('temp_tissue_type') }},
        molecular_genetics = nullif(molecular_genetics, ''),
        role_in_cancer = {{ list_of('temp_role_in_cancer') }},
        mutation_types = {{ list_of('temp_mutation_types') }},
        translocation_partner = {{ list_of('temp_translocation_partner') }},
        other_germline_mutation = IF(temp_other_germline_mut = 'y', 1, 0),
        other_syndrome = {{ list_of('temp_other_syndrome') }},
        synonyms = {{ list_of('temp_synonyms') }}
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
