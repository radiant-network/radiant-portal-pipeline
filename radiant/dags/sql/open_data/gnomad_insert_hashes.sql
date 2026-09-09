-- OpenDataLake publishes no `locus_hash`: recomputed here, byte-identical to the VCF ingest path
-- (radiant/tasks/vcf/snv/common.py) and to raw_exomiser's generated column. It is computed once, in a
-- subquery, so the join key and the projected column share a single evaluation instead of the two
-- separate projections StarRocks would otherwise plan. The GET_VARIANT_ID filter sits inside that
-- subquery so only the variants that need a surrogate id are hashed at all.
INSERT INTO {{ mapping.starrocks_variant_lookup }}(`locus_hash`)
SELECT h.locus_hash
FROM (
    SELECT sha2(concat_ws('-', src.chromosome, src.start, src.reference, src.alternate), 256) AS locus_hash
    FROM {{ mapping.iceberg_gnomad_joint }} src
    WHERE GET_VARIANT_ID(src.chromosome, src.start, src.reference, src.alternate) IS NULL
) h
LEFT ANTI JOIN {{ mapping.starrocks_variant_lookup }} vd ON vd.locus_hash = h.locus_hash;
