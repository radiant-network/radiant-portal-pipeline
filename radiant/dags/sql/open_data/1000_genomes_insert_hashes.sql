INSERT INTO {{ mapping.starrocks_variant_lookup }}(locus_hash)
SELECT
    locus_hash
FROM {{ mapping.iceberg_1000_genomes }} g
LEFT ANTI JOIN {{ mapping.starrocks_variant_lookup }} vd ON vd.locus_hash = g.locus_hash
WHERE GET_VARIANT_ID(g.chromosome, g.start, g.reference, g.alternate) IS NULL;