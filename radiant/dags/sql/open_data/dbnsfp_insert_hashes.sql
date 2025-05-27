INSERT INTO {{ params.starrocks_variant_lookup }}(`locus_hash`)
SELECT
    `locus_hash`
FROM {{ params.iceberg_dbnsfp }} d
LEFT ANTI JOIN {{ params.starrocks_variant_lookup }} vd ON vd.locus_hash=d.locus_hash
WHERE GET_VARIANT_ID(d.chromosome, d.start, d.reference, d.alternate) IS NULL;