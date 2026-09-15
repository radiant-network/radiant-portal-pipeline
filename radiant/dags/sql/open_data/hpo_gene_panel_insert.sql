-- `hpo_genes_v1` stays faithful to the HPO source file, so it carries the upstream column names
-- (gene_symbol / hpo_name / hpo_id) rather than the ones the legacy Radiant table used
-- (symbol / hpo_term_name / hpo_term_id). Renamed here, on the consumer side, by design
-- (radiant-open-datalake spark/doc/release-notes/hpo_genes/v1.md).
--
-- The branch covers the case where RADIANT_OPEN_DATA_CONTRACT_TABLES leaves `hpo_genes` out and the
-- source falls back to the pre-contract `hpo_gene_set`, which already uses the Radiant names.
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
