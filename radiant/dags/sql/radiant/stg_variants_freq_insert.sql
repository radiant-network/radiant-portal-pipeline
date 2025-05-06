INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ params.starrocks_staging_variants_frequencies }}
SELECT
    o.part,
    o.locus_id,
    COUNT(DISTINCT patient_id) AS pc,
    SUM(array_sum(array_filter(x -> x = 1, calls))) AS ac,
    COUNT(CASE zygosity WHEN 'HOM' THEN 1 ELSE NULL END) AS hom
FROM {{ params.starrocks_occurrences }} o JOIN {{ params.source_sequencing_experiments }} s ON s.seq_id=o.seq_id
WHERE o.part = %(part)s
GROUP BY locus_id, o.part
