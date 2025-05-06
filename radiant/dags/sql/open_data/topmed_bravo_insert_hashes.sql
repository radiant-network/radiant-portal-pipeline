INSERT INTO {{ params.starrocks_variants_lookup }}(`locus_hash`)
select
    `locus_hash`
FROM {{ params.iceberg_topmed_bravo }} v
LEFT ANTI JOIN {{ params.starrocks_variants_lookup }} vd ON vd.locus_hash=v.locus_hash;