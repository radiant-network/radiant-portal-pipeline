INSERT INTO {{ params.starrocks_variants_lookup }}(`hash`)
SELECT
    `hash`
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.{{ params.iceberg_dbnsfp }} d
LEFT ANTI JOIN {{ params.starrocks_variants_lookup }} vd ON vd.hash=d.hash;