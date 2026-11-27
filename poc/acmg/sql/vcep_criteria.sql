-- Variants a ClinGen VCEP curated (clingen_erepo, matched on ClinVar's variation id) take its criteria in place of the
-- computed ones of the variant, except on frequency (BA1, BS1, PM2): when the panel and the computed criteria point in
-- opposite directions (PM2 against BA1 or BS1), our frequency criteria stay (newer gnomAD) and the panel's are dropped;
-- otherwise the panel's replace ours (gene-specific thresholds). The case's own criteria stay unless the panel applied
-- the same one: a panel PS2 or PM6 is the de novo evidence of our PM6, so it drops it.
-- Runs after the variant and occurrence steps, before the classification: a rerun without the VCEP row restores them.
DELETE FROM ${output_db}.acmg_variant_criteria
WHERE criterion NOT IN ('BA1', 'BS1', 'PM2')
    AND locus_id IN (
        SELECT cv.locus_id FROM ${shared_db}.clinvar cv
        JOIN ${output_db}.clingen_erepo e ON e.variation_id = cv.name
        WHERE cv.locus_id IN (SELECT locus_id FROM (${classified}) k)
    );

DELETE FROM ${output_db}.acmg_variant_criteria
WHERE criterion IN ('BA1', 'BS1', 'PM2')
    AND locus_id IN (
        SELECT cv.locus_id FROM ${shared_db}.clinvar cv
        JOIN ${output_db}.clingen_erepo e ON e.variation_id = cv.name
        WHERE cv.locus_id IN (SELECT locus_id FROM (${classified}) k)
    )
    AND locus_id NOT IN (
        SELECT cv.locus_id FROM ${shared_db}.clinvar cv
        JOIN ${output_db}.clingen_erepo e ON e.variation_id = cv.name
        JOIN (
            SELECT locus_id, max(IF(criterion = 'PM2', 1, 0)) AS pm2, max(IF(criterion IN ('BA1', 'BS1'), 1, 0)) AS bs
            FROM ${output_db}.acmg_variant_criteria
            GROUP BY locus_id
        ) o ON o.locus_id = cv.locus_id
        -- Our PM2 only stands against a panel's BA1/BS1 where gnomAD covers the position: absent is not rare.
        LEFT JOIN ${shared_db}.gnomad_genomes_v3 g ON g.locus_id = cv.locus_id
        WHERE (o.bs = 1 AND array_contains(array_map(c -> c.criterion, e.criteria), 'PM2'))
            OR (o.pm2 = 1 AND g.an >= ${min_an}
                AND arrays_overlap(array_map(c -> c.criterion, e.criteria), ['BA1', 'BS1']))
    );

DELETE FROM ${output_db}.acmg_occurrence_criteria
WHERE seq_id IN (${cases})
    AND concat(locus_id, ':', criterion) IN (
        SELECT concat(cv.locus_id, ':', IF(u.c.criterion = 'PS2', 'PM6', u.c.criterion))
        FROM ${shared_db}.clinvar cv
        JOIN ${output_db}.clingen_erepo e ON e.variation_id = cv.name
        CROSS JOIN unnest(e.criteria) AS u(c)
    );

-- Frequency criteria still on a curated variant are ours, kept on a contradiction: the panel's are left out there.
INSERT INTO ${output_db}.acmg_variant_criteria
    (locus_id, transcript_id, criterion, code, strength, points, value, threshold, source, details, refs,
     ruleset_version)
SELECT DISTINCT
    k.locus_id, k.transcript_id, u.c.criterion, u.c.code, u.c.strength, u.c.points,
    CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), 'clingen_vcep',
    CAST([] AS ARRAY<STRUCT<name VARCHAR(50), value DOUBLE>>), e.pmids, '${ruleset_version}'
FROM (${classified}) k
JOIN ${shared_db}.clinvar cv ON cv.locus_id = k.locus_id
JOIN ${output_db}.clingen_erepo e ON e.variation_id = cv.name
CROSS JOIN unnest(e.criteria) AS u(c)
LEFT JOIN (
    SELECT DISTINCT locus_id FROM ${output_db}.acmg_variant_criteria WHERE criterion IN ('BA1', 'BS1', 'PM2')
) ours ON ours.locus_id = k.locus_id
WHERE NOT (u.c.criterion IN ('BA1', 'BS1', 'PM2') AND ours.locus_id IS NOT NULL)
