LOAD LABEL {{ database_name }}.{{ load_label }}
(
    DATA INFILE(%(tsv_filepath)s)
    INTO TABLE {{ table_name }} {{ temporary_partition_clause }}
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "CSV"
    (
        skip_header = 1
    )
    (rank, id, gene_symbol, entrez_gene_id, moi, temp_p_value, gene_combined_score, temp_exomiser_pheno_score,
     temp_gene_variant_score,variant_score, contributing_variant, temp_whitelist_variant, temp_vcf_id, temp_rs_id, chromosome, start, end, reference, alternate,
     temp_change_length, temp_qual, temp_filter, temp_genotype, temp_functional_class, temp_hgvs, acmg_classification, acmg_evidence,
     temp_acmg_disease_id,temp_acmg_disease_name,temp_clinvar_variation_id,temp_clinvar_primary_interpretation,
     temp_clinvar_star_rating, temp_gene_constraint_loeuf,temp_gene_constraint_loeuf_lower, temp_gene_constraint_loeuf_upper,
     temp_max_freq_source, temp_max_freq, temp_all_freq, temp_max_path_source, temp_max_path, temp_all_path)
    SET
    (
        part = %(part)s,
        seq_id = %(seq_id)s,
        id = id,
        rank = rank,
        symbol = gene_symbol,
        entrez_gene_id = entrez_gene_id,
        moi = moi,
        gene_combined_score = gene_combined_score,
        variant_score = variant_score,
        contributing_variant = contributing_variant,
        chromosome = chromosome,
        start = start,
        end = end,
        reference = reference,
        alternate = alternate,
        acmg_classification = lower(acmg_classification),
        acmg_evidence = IF(acmg_evidence = "", NULL, split(acmg_evidence, ","))
     )
     where contributing_variant=1
 )
 WITH BROKER
 (
        {{ broker_configuration }}
 )
PROPERTIES
(
    'timeout' = '{{ broker_load_timeout }}'
);