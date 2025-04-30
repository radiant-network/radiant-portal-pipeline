INSERT INTO variant_dict(`hash`)
SELECT
    `hash`
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.gnomad_genomes_v3 v
LEFT ANTI JOIN variant_dict vd ON vd.hash=v.hash ;