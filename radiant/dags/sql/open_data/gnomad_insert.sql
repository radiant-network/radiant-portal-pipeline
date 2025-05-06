INSERT OVERWRITE {{ params.starrocks_gnomad_genomes_v3 }}
SELECT
    v.locus_id,
    t.af
FROM {{ params.iceberg_gnomad_genomes_v3 }} t
JOIN {{ params.starrocks_variants_lookup }} v ON t.locus_hash = v.locus_hash
