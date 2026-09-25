-- One row per (gene, germline tumour type), read from the StarRocks cosmic_gene_set table that
-- radiant-import-cosmic-gene-set loads from the COSMIC census TSV.
INSERT OVERWRITE {{ mapping.starrocks_cosmic_gene_panel }}
SELECT symbol,
       unnest AS panel
FROM {{ mapping.starrocks_cosmic_gene_set }}, unnest(tumour_types_germline)
WHERE tumour_types_germline IS NOT NULL
;
