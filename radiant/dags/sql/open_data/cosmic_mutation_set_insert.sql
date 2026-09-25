-- One row per locus: the census lists a mutation once per transcript, so a locus repeats (same or different
-- COSV id); keep the row with the most mutated samples, as the legacy Spark ETL did (Variants.scala withCosmic).
INSERT OVERWRITE {{ mapping.starrocks_cosmic_mutation_set }}
SELECT
    locus_id,
    mutation_url,
    shared_aa,
    cosmic_id,
    sample_mutated,
    sample_tested,
    tier,
    sample_mutated / sample_tested AS sample_ratio
FROM (
    SELECT
        COALESCE(GET_VARIANT_ID(t.chromosome, t.start, t.reference, t.alternate), v.locus_id) AS locus_id,
        t.mutation_url,
        t.shared_aa,
        t.cosmic_id,
        t.sample_mutated,
        t.sample_tested,
        t.tier,
        ROW_NUMBER() OVER (PARTITION BY t.locus_hash ORDER BY t.sample_mutated DESC, t.cosmic_id) AS rn
    FROM {{ mapping.starrocks_raw_cosmic_mutation_set }} t
    LEFT JOIN {{ mapping.starrocks_variant_lookup }} v ON v.locus_hash = t.locus_hash
) ranked
WHERE rn = 1;
