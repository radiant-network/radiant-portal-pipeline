INSERT INTO topmed_bravo
SELECT
	v.locus_id,
	t.af
FROM iceberg.poc_starrocks.topmed_bravo t
JOIN variant_dict v ON t.hash = v.hash
LEFT ANTI JOIN topmed_bravo br ON br.locus_id = v.locus_id