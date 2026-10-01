-- One row per (transcript, c. change) for the mutations cosmic_mutation_set cannot hold: the staging rows
-- the normalizer left without a locus. Same tie-break as cosmic_mutation_set: the most mutated samples win.
INSERT OVERWRITE {{ mapping.starrocks_cosmic_mutation_set_hgvs }}
SELECT
    transcript_id,
    cds_change,
    mutation_url,
    shared_aa,
    cosmic_id,
    sample_mutated,
    sample_tested,
    tier,
    sample_mutated / sample_tested AS sample_ratio
FROM (
    SELECT
        t.transcript_id,
        t.cds_change,
        t.mutation_url,
        t.shared_aa,
        t.cosmic_id,
        t.sample_mutated,
        t.sample_tested,
        t.tier,
        ROW_NUMBER() OVER (PARTITION BY t.transcript_id, t.cds_change ORDER BY t.sample_mutated DESC, t.cosmic_id) AS rn
    FROM {{ mapping.starrocks_raw_cosmic_mutation_set }} t
    WHERE t.locus_hash IS NULL
) ranked
WHERE rn = 1;
