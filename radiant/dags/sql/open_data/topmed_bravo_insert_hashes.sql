INSERT INTO {{ mapping.starrocks_variant_lookup }}(`locus_hash`)
SELECT src.locus_hash
FROM {{ mapping.iceberg_topmed_bravo }} src
LEFT ANTI JOIN {{ mapping.starrocks_variant_lookup }} vd ON vd.locus_hash = src.locus_hash
WHERE GET_VARIANT_ID(src.chromosome, src.start, src.reference, src.alternate) IS NULL;
