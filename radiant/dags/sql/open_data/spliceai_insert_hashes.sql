INSERT INTO {{ params.starrocks_variants_lookup }}(`locus_hash`)
SELECT
    `locus_hash`
FROM {{ params.iceberg_spliceai }} s
LEFT ANTI JOIN {{ params.starrocks_variants_lookup }} vd ON vd.locus_hash=s.locus_hash;