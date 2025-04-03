INSERT INTO variant_dict(hash)
SELECT
    hash
FROM iceberg.poc_starrocks.1000_genomes g
LEFT ANTI JOIN variant_dict vd ON vd.hash = g.hash;