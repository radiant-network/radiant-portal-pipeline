INSERT INTO {{ params.starrocks_variants_lookup }}(`locus_hash`)
SELECT
    `locus_hash`
FROM {{ params.iceberg_gnomad_constraints }} v
LEFT ANTI JOIN {{ params.starrocks_variants_lookup }} vd ON vd.locus_hash=v.locus_hash ;