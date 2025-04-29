INSERT INTO variant_dict(hash)
SELECT
    hash
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.1000_genomes g
LEFT ANTI JOIN variant_dict vd ON vd.hash = g.hash;