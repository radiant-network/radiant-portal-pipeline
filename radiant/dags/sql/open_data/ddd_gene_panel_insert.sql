INSERT OVERWRITE {{ mapping.starrocks_ddd_gene_panel }}
SELECT symbol,
       disease_name as panel
FROM {{ mapping.iceberg_ddd_gene_set }}
where disease_name is not null
;

