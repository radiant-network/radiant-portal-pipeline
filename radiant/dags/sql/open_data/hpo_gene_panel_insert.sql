INSERT OVERWRITE {{ mapping.starrocks_hpo_gene_panel }}
{% if mapping.iceberg_hpo_gene_set_is_contract %}
SELECT distinct h.gene_symbol AS symbol,
       concat(h.hpo_name, '(', h.hpo_id, ')') AS panel,
       h.hpo_name AS hpo_term_name,
       h.hpo_id AS hpo_term_id
FROM {{ mapping.iceberg_hpo_gene_set }} h
WHERE h.gene_symbol IS NOT NULL AND h.hpo_name IS NOT NULL AND h.hpo_id IS NOT NULL
{% else %}
SELECT distinct h.symbol,
       concat(h.hpo_term_name, '(', h.hpo_term_id, ')') AS panel,
       h.hpo_term_name,
       h.hpo_term_id
FROM {{ mapping.iceberg_hpo_gene_set }} h
WHERE h.symbol IS NOT NULL AND h.hpo_term_name IS NOT NULL AND h.hpo_term_id IS NOT NULL
{% endif %}
;
