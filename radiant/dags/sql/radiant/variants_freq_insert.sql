INSERT INTO {{ params.starrocks_variants_frequencies }}
SELECT
    o.locus_id,
    COUNT(1) AS pc,
    SUM(ARRAY_SUM(ARRAY_FILTER(x -> x = 1, calls))) AS ac,
    COUNT(CASE WHEN zygosity = 'HOM' THEN 1 END) AS hom
FROM {{ params.starrocks_occurrences }} o
WHERE has_alt
GROUP BY o.locus_id;