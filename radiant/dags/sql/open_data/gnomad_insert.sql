INSERT OVERWRITE {{ params.starrocks_gnomad_genomes_v3 }}
SELECT
    COALESCE(GET_VARIANT_ID(t.chromosome, t.start, t.reference, t.alternate), v.locus_id) as locus_id,
    t.af
FROM {{ params.iceberg_gnomad_genomes_v3 }} t
LEFT JOIN {{ params.starrocks_variants_lookup }} v ON t.locus_hash = v.locus_hash
