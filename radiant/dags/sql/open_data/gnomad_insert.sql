INSERT OVERWRITE {{ mapping.starrocks_gnomad_genomes_v3 }}
SELECT
    COALESCE(GET_VARIANT_ID(t.chromosome, t.start, t.reference, t.alternate), v.locus_id) as locus_id,
    t.af,
    t.ac,
    t.an,
    t.nhomalt
FROM {{ mapping.iceberg_gnomad_genomes_v3 }} t
LEFT JOIN {{ mapping.starrocks_variant_lookup }} v ON t.locus_hash = v.locus_hash
