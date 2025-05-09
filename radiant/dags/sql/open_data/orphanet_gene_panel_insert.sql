INSERT OVERWRITE {{ params.starrocks_orphanet_gene_panel }}
SELECT gene_symbol as symbol,
       name as panel,
       disorder_id,
       type_of_inheritance as inheritance
FROM {{ params.iceberg_orphanet_gene_set }}
;

