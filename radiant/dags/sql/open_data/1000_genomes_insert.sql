INSERT OVERWRITE {{ params.starrocks_1000_genomes }}
SELECT
    COALESCE(GET_VARIANT_ID(tg.chromosome, tg.start, tg.reference, tg.alternate), v.locus_id) as locus_id,
    tg.af
FROM {{ params.iceberg_1000_genomes }} tg
LEFT JOIN {{ params.starrocks_variants_lookup }} v ON tg.locus_hash = v.locus_hash;
