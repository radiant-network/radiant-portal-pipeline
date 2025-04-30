INSERT INTO {{ params.starrocks_topmed_bravo }}
SELECT
	v.locus_id,
	t.af
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.{{ params.iceberg_topmed_bravo }} t
JOIN {{ params.starrocks_variants_lookup }} v ON t.hash = v.hash
LEFT ANTI JOIN {{ params.starrocks_topmed_bravo }} br ON br.locus_id = v.locus_id