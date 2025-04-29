INSERT INTO variant_dict(`hash`)
SELECT
    `hash`
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.kf_variants v
LEFT ANTI JOIN variant_dict vd ON vd.hash=v.hash;