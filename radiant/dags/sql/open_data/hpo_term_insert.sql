INSERT OVERWRITE {{ mapping.starrocks_hpo_term }}
SELECT id, name term
FROM {{ mapping.iceberg_hpo_term }}
;

