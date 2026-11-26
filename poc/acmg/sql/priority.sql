-- Priority of each classified occurrence of the cases: ACMG points + PP4 of its gene for the case, as a probability
-- of pathogenicity: odds = very_strong_odds ^ (points / 8), probability = odds * prior / ((odds - 1) * prior + 1).
DELETE FROM ${output_db}.acmg_priority WHERE seq_id IN (${cases});

INSERT INTO ${output_db}.acmg_priority
    (seq_id, locus_id, transcript_id, pp4_points, priority_points, priority_probability, ruleset_version)
SELECT
    seq_id, locus_id, transcript_id, pp4_points, priority_points,
    pow(${very_strong_odds}, priority_points / 8.0) * ${prior}
        / ((pow(${very_strong_odds}, priority_points / 8.0) - 1) * ${prior} + 1),
    '${ruleset_version}'
FROM (
    SELECT
        c.seq_id, c.locus_id, c.transcript_id,
        coalesce(p.pp4_points, 0) AS pp4_points,
        c.acmg_points + coalesce(p.pp4_points, 0) AS priority_points
    FROM ${output_db}.acmg_classification c
    JOIN ${shared_db}.snv__consequence k ON k.locus_id = c.locus_id AND k.transcript_id = c.transcript_id
    LEFT JOIN ${output_db}.acmg_phenotype p ON p.seq_id = c.seq_id AND p.symbol = k.symbol
    WHERE c.seq_id IN (${cases})
) t;
