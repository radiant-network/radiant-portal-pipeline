INSERT OVERWRITE {{ params.starrocks_hpo_term }}
SELECT id, name term
FROM {{ params.iceberg_hpo_term }}
;

