INSERT INTO {{ params.starrocks_clinvar }}
SELECT
	v.locus_id,
	c.*
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.{{ params.iceberg_clinvar }} c
JOIN {{ params.starrocks_variants_lookup }} v ON c.hash = v.hash
LEFT ANTI JOIN {{ params.starrocks_clinvar }} c ON c.locus_id = v.locus_id