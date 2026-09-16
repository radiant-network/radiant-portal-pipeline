INSERT OVERWRITE {{ mapping.starrocks_mondo_term }}
SELECT id, name term
FROM {{ mapping.iceberg_mondo_term }}
;

