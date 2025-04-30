INSERT INTO {{ params.starrocks_variants_lookup }}(`hash`)
SELECT
    `hash`
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.{{ params.iceberg_spliceai }} s
LEFT ANTI JOIN {{ params.starrocks_variants_lookup }} vd ON vd.hash=s.hash;