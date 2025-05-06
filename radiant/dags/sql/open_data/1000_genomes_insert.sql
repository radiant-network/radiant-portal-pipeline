INSERT OVERWRITE {{ params.starrocks_1000_genomes }}
SELECT
    v.locus_id,
    tg.af
FROM {{ params.iceberg_1000_genomes }} tg
JOIN {{ params.starrocks_variants_lookup }} v ON tg.locus_hash = v.locus_hash
