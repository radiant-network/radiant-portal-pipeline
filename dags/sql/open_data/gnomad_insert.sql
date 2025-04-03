INSERT INTO gnomad_genomes_v3
SELECT
    v.locus_id,
    t.af
FROM iceberg.poc_starrocks.gnomad_genomes_v3 t
JOIN variant_dict v ON t.hash = v.hash
LEFT ANTI JOIN gnomad_genomes_v3 g ON g.locus_id = v.locus_id