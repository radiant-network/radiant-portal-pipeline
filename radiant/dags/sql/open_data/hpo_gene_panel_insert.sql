-- `hpo_genes_v1` stays faithful to the HPO source file, so it carries the upstream column names
-- (gene_symbol / hpo_name / hpo_id) rather than the ones the legacy Radiant table used
-- (symbol / hpo_term_name / hpo_term_id). Renamed here, on the consumer side, by design
-- (radiant-open-datalake spark/doc/release-notes/hpo_genes/v1.md).
INSERT OVERWRITE {{ mapping.starrocks_hpo_gene_panel }}
SELECT distinct h.gene_symbol AS symbol,
       concat(h.hpo_name, '(', h.hpo_id, ')') AS panel,
       h.hpo_name AS hpo_term_name,
       h.hpo_id AS hpo_term_id
FROM {{ mapping.iceberg_hpo_gene_set }} h
WHERE h.gene_symbol IS NOT NULL AND h.hpo_name IS NOT NULL AND h.hpo_id IS NOT NULL
;
