INSERT OVERWRITE {{ params.starrocks_mondo_term }}
SELECT id, name term
FROM {{ params.iceberg_mondo_term }}
;

