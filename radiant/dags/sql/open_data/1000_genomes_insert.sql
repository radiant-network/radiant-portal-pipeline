INSERT OVERWRITE {{ mapping.starrocks_1000_genomes }}
SELECT
    COALESCE(GET_VARIANT_ID(tg.chromosome, tg.start, tg.reference, tg.alternate), v.locus_id) as locus_id,
    tg.af,
    tg.ac,
    tg.an
FROM {{ mapping.iceberg_1000_genomes }} tg
LEFT JOIN {{ mapping.starrocks_variant_lookup }} v
ON v.locus_hash = {% if mapping.iceberg_1000_genomes_is_contract %}sha2(concat_ws('-', tg.chromosome, tg.start, tg.reference, tg.alternate), 256){% else %}tg.locus_hash{% endif %};
