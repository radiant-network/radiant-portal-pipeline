INSERT INTO clinvar
SELECT
	v.locus_id,
	c.*
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.clinvar c
JOIN variant_dict v ON c.hash = v.hash
LEFT ANTI JOIN clinvar c ON c.locus_id = v.locus_id