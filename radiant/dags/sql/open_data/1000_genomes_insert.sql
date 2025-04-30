INSERT INTO {{ params.starrocks_1000_genomes }}
SELECT
    v.locus_id,
    tg.af
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.{{ params.iceberg_1000_genomes }} tg
JOIN {{ params.starrocks_variants_lookup }} v ON tg.hash = v.hash
LEFT ANTI JOIN {{ params.starrocks_1000_genomes }} g ON g.locus_id = v.locus_id