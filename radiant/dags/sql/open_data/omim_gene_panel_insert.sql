INSERT OVERWRITE {{ params.starrocks_omim_gene_panel }}
SELECT
    unnest as symbol,
    phenotype.name as panel,
    phenotype.inheritance_code as inheritance_code,
    phenotype.inheritance as inheritance,
    omim_gene_id as omim_gene_id,
    phenotype.omim_id as omim_phenotype_id
FROM {{ params.iceberg_omim_gene_set }}, unnest(symbols);

