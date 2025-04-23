INSERT INTO variants_freq
SELECT
    o.locus_id,
    COUNT(1) AS pc,
    SUM(ARRAY_SUM(ARRAY_FILTER(x -> x = 1, calls))) AS ac,
    COUNT(CASE WHEN zygosity = 'HOM' THEN 1 END) AS hom
FROM occurrences o
WHERE has_alt
GROUP BY o.locus_id;