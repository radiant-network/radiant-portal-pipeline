-- OpenDataLake publishes no `locus_hash`: recomputed here, byte-identical to the VCF ingest path
-- (radiant/tasks/vcf/snv/common.py) and to raw_exomiser's generated column. It is computed once, in a
-- subquery, so the join key and the projected column share a single evaluation instead of the two
-- separate projections StarRocks would otherwise plan. The GET_VARIANT_ID filter sits inside that
-- subquery so only the variants that need a surrogate id are hashed at all.
INSERT OVERWRITE {{ mapping.starrocks_clinvar }}
SELECT
    COALESCE(GET_VARIANT_ID(c.chromosome, c.start, c.reference, c.alternate), v.locus_id) as locus_id,
    c.chromosome,
    c.start,
    c.end,
    c.reference,
    c.alternate,
    c.interpretations,
    c.name,
    c.clin_sig,
    c.clin_sig_conflict,
    c.af_exac,
    c.clnvcso,
    c.geneinfo,
    c.clnsigincl,
    c.clnvi,
    c.clndisdb,
    c.clnrevstat,
    c.alleleid,
    c.origin,
    c.clndnincl,
    c.rs,
    c.dbvarid,
    c.af_tgp,
    c.clnvc,
    c.clnhgvs,
    c.mc,
    c.af_esp,
    c.clndisdbincl,
    c.conditions,
    c.inheritance,
    c.locus,
    c.locus_hash
FROM (
    SELECT src.*,
           concat_ws('-', src.chromosome, src.start, src.reference, src.alternate) AS locus,
           sha2(concat_ws('-', src.chromosome, src.start, src.reference, src.alternate), 256) AS locus_hash
    FROM {{ mapping.iceberg_clinvar }} src
) c
LEFT JOIN {{ mapping.starrocks_variant_lookup }} v ON v.locus_hash = c.locus_hash
