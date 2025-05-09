INSERT OVERWRITE {{ params.starrocks_hpo_gene_panel }}
SELECT symbol,
       concat(hpo_term_name, '(', hpo_term_id, ')') AS panel,
       hpo_term_name,
       hpo_term_id
FROM {{ params.iceberg_hpo_gene_set }}
;

