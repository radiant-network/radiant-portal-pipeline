INSERT INTO {{ params.starrocks_variants_lookup }}(`hash`)
select
    `hash`
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.{{ params.iceberg_topmed_bravo }} v
LEFT ANTI JOIN {{ params.starrocks_variants_lookup }} vd ON vd.hash=v.hash;