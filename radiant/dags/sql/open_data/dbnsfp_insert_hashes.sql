INSERT INTO variant_dict(`hash`)
SELECT
    `hash`
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.dbnsfp d
LEFT ANTI JOIN variant_dict vd ON vd.hash=d.hash;