INSERT INTO spliceai
SELECT
    v.locus_id,
    s.symbol,
    s.max_score.ds AS spliceai_ds,
    s.max_score.type AS spliceai_type
FROM iceberg.poc_starrocks.spliceai_enriched s
JOIN variant_dict v ON s.hash = v.hash
LEFT ANTI JOIN spliceai sp ON sp.locus_id = v.locus_id