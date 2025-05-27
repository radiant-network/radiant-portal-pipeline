INSERT OVERWRITE {{ params.starrocks_topmed_bravo }}
SELECT
    COALESCE(GET_VARIANT_ID(t.chromosome, t.start, t.reference, t.alternate), v.locus_id) as locus_id,
	t.af
FROM {{ params.iceberg_topmed_bravo }} t
LEFT JOIN {{ params.starrocks_variant_lookup }} v ON t.locus_hash = v.locus_hash
