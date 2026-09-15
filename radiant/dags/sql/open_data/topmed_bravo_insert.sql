INSERT OVERWRITE {{ mapping.starrocks_topmed_bravo }}
SELECT
    COALESCE(GET_VARIANT_ID(t.chromosome, t.start, t.reference, t.alternate), v.locus_id) as locus_id,
	t.af,
    t.ac,
    t.an,
    t.homozygotes
FROM {{ mapping.iceberg_topmed_bravo }} t
LEFT JOIN {{ mapping.starrocks_variant_lookup }} v ON v.locus_hash = {% if mapping.iceberg_topmed_bravo_is_contract %}sha2(concat_ws('-', t.chromosome, t.start, t.reference, t.alternate), 256){% else %}t.locus_hash{% endif %}
