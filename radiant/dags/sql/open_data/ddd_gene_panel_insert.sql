INSERT OVERWRITE {{ params.starrocks_ddd_gene_panel }}
SELECT symbol,
       disease_name as panel
FROM {{ params.iceberg_ddd_gene_set }}
where disease_name is not null
;

