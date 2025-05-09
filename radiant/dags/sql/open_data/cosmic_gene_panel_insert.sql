INSERT OVERWRITE {{ params.starrocks_cosmic_gene_panel }}
SELECT symbol,
       unnest AS panel
FROM {{ params.iceberg_cosmic_gene_set }}, unnest(tumour_types_germline)
WHERE tumour_types_germline IS NOT NULL
;

