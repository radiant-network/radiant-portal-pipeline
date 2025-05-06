INSERT OVERWRITE {{ params.starrocks_topmed_bravo }}
SELECT
	v.locus_id,
	t.af
FROM {{ params.iceberg_topmed_bravo }} t
JOIN {{ params.starrocks_variants_lookup }} v ON t.locus_hash = v.locus_hash
