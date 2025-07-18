INSERT OVERWRITE {{ params.starrocks_dbsnp }}
SELECT
    COALESCE(GET_VARIANT_ID(d.chromosome, d.start, d.reference, d.alternate), v.locus_id) as locus_id,
	d.name
FROM {{ params.iceberg_dbsnp }} d
LEFT JOIN {{ params.starrocks_variant_lookup }} v ON d.locus_hash = v.locus_hash