{#- The SNV Exomiser reports (`variants.tsv`) of a batch of `radiant_germline_annotation` tasks, in one load job: one
    `data_desc` per report. The report has no task_id, seq_id nor tenant_code: each data_desc adds its task's values
    in its SET. One job per batch, not one per task: a broker load has a fixed cost (FE scheduling, the cluster's
    shared load queue, its own transaction) that made the load grow with the task count. The data_descs of one table
    make one atomic job (starrocks docs BROKER_LOAD.md, `data_desc`): a bad report cancels the whole batch.
    The `raw` block is rendered at the second render only (per job, in run_insert), with `file_count`, `table_name`
    and `temporary_partition_clause`. #}
LOAD LABEL {{ database_name }}.{{ load_label }}
(
{% raw %}{% for i in range(file_count) %}
    DATA INFILE(%(filepath_{{ i }})s)
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
        seq_id = %(seq_id_{{ i }})s,
        task_id = %(task_id_{{ i }})s,
        tenant_code = %(tenant_code_{{ i }})s,
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
    WHERE contributing_variant = 1{{ "," if not loop.last }}
{% endfor %}{% endraw %}
)
WITH BROKER
(
    {{ broker_configuration }}
)
PROPERTIES
(
    'timeout' = '{{ broker_load_timeout }}'
);
