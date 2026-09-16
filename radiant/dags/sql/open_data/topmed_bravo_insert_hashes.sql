INSERT INTO {{ mapping.starrocks_variant_lookup }}(`locus_hash`)
select
    `locus_hash`
FROM {{ mapping.iceberg_topmed_bravo }} v
LEFT ANTI JOIN {{ mapping.starrocks_variant_lookup }} vd ON vd.locus_hash=v.locus_hash
WHERE GET_VARIANT_ID(v.chromosome, v.start, v.reference, v.alternate) IS NULL;