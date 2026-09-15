INSERT OVERWRITE {{ mapping.starrocks_dbsnp }}
SELECT
    COALESCE(GET_VARIANT_ID(d.chromosome, d.start, d.reference, d.alternate), v.locus_id) as locus_id,
	d.name
FROM {{ mapping.iceberg_dbsnp }} d
LEFT JOIN {{ mapping.starrocks_variant_lookup }} v ON v.locus_hash = {% if mapping.iceberg_dbsnp_is_contract %}sha2(concat_ws('-', d.chromosome, d.start, d.reference, d.alternate), 256){% else %}d.locus_hash{% endif %}