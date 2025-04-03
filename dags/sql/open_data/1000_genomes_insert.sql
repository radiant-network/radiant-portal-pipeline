INSERT INTO 1000_genomes
SELECT
    v.locus_id,
    tg.af
FROM iceberg.poc_starrocks.1000_genomes tg
JOIN variant_dict v ON tg.hash = v.hash
LEFT ANTI JOIN 1000_genomes g ON g.locus_id = v.locus_id