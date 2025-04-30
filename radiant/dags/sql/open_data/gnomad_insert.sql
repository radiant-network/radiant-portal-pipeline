INSERT INTO {{ params.starrocks_gnomad_genomes_v3 }}
SELECT
    v.locus_id,
    t.af
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.{{ params.iceberg_gnomad_genomes_v3 }} t
JOIN {{ params.starrocks_variants_lookup }} v ON t.hash = v.hash
LEFT ANTI JOIN {{ params.starrocks_gnomad_genomes_v3 }} g ON g.locus_id = v.locus_id