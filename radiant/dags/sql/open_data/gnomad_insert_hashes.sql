INSERT INTO {{ params.starrocks_variants_lookup }}(`locus_hash`)
SELECT
    `locus_hash`
FROM {{ params.iceberg_gnomad_genomes_v3 }} v
LEFT ANTI JOIN {{ params.starrocks_variants_lookup }} vd ON vd.locus_hash=v.locus_hash ;