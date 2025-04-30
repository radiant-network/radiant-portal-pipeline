INSERT INTO {{ params.starrocks_variants_lookup }}(`hash`)
SELECT
    `hash`
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.{{ params.iceberg_gnomad_genomes_v3 }} v
LEFT ANTI JOIN {{ params.starrocks_variants_lookup }} vd ON vd.hash=v.hash ;