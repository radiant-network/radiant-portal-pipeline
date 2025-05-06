INSERT OVERWRITE {{ params.starrocks_spliceai }}
SELECT
    v.locus_id,
    s.symbol,
    s.max_score.ds AS spliceai_ds,
    s.max_score.type AS spliceai_type
FROM {{ params.iceberg_spliceai }} s
JOIN {{ params.starrocks_variants_lookup }} v ON s.locus_hash = v.locus_hash
