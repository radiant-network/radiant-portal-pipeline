INSERT INTO clinvar
SELECT
	v.locus_id,
	c.*
FROM iceberg.poc_starrocks.clinvar c
JOIN variant_dict v ON c.hash = v.hash
LEFT ANTI JOIN clinvar c ON c.locus_id = v.locus_id