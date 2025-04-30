INSERT INTO {{ params.starrocks_spliceai }}
SELECT
    v.locus_id,
    s.symbol,
    s.max_score.ds AS spliceai_ds,
    s.max_score.type AS spliceai_type
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.{{ params.iceberg_spliceai }} s
JOIN {{ params.starrocks_variants_lookup }} v ON s.hash = v.hash
LEFT ANTI JOIN {{ params.starrocks_spliceai }} sp ON sp.locus_id = v.locus_id