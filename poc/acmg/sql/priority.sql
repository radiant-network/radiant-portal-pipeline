-- Priority of each classified occurrence of the cases: ACMG points + PP4 of its gene for the case + the genotype's
-- fit to the gene's inheritance, as a probability of pathogenicity:
-- odds = very_strong_odds ^ (points / 8), probability = odds * prior / ((odds - 1) * prior + 1).
DELETE FROM ${output_db}.acmg_priority WHERE seq_id IN (${cases});

INSERT INTO ${output_db}.acmg_priority
    (seq_id, locus_id, transcript_id, pp4_points, inheritance_points, priority_points, priority_probability,
     ruleset_version)
WITH case_parents AS (
    SELECT
        p.seq_id,
        max(IF(s.family_role = 'father', s.affected_status, NULL)) AS father_status,
        max(IF(s.family_role = 'mother', s.affected_status, NULL)) AS mother_status
    FROM ${shared_db}.staging_sequencing_experiment p
    JOIN ${shared_db}.staging_sequencing_experiment s
        ON s.case_id = p.case_id AND s.analysis_type = 'germline' AND NOT s.deleted
    WHERE p.seq_id IN (${cases}) AND NOT p.deleted
    GROUP BY p.seq_id
),
-- Parent calls: max allele 0 = called reference, >= 1 = carrier; a no-call (-1) or no parent says nothing.
genotype AS (
    SELECT
        o.seq_id,
        o.locus_id,
        max(o.zygosity) AS zygosity,
        max(IF((cp.father_status = 'non_affected' AND array_max(o.father_calls) >= 1)
            OR (cp.mother_status = 'non_affected' AND array_max(o.mother_calls) >= 1), 1, 0)) = 1 AS healthy_carrier_parent
    FROM ${tenant_db}.germline__snv__occurrence o
    JOIN case_parents cp ON cp.seq_id = o.seq_id
    WHERE o.seq_id IN (${cases})
    GROUP BY o.seq_id, o.locus_id
),
-- A comphet partner classed VUS or above: a second allele that may complete a recessive genotype.
second_allele AS (
    SELECT DISTINCT ch.seq_id, ch.locus_id
    FROM ${tenant_db}.germline__snv__comphet ch
    JOIN ${output_db}.acmg_classification pc ON pc.seq_id = ch.seq_id AND pc.locus_id = ch.partner_locus_id
    WHERE ch.seq_id IN (${cases}) AND pc.acmg_class IN ('Pathogenic', 'Likely pathogenic', 'VUS')
),
-- OMIM inheritance of the classified transcript's gene (see variant_facts.sql).
omim AS (
    SELECT symbol, array_remove(array_unique_agg(inheritance_code), NULL) AS inheritance_code
    FROM ${shared_db}.omim_gene_panel
    GROUP BY symbol
),
scored AS (
    SELECT
        c.seq_id, c.locus_id, c.transcript_id,
        -- A code is the criterion plus an optional strength (BP4_Moderate): compare the criterion.
        IF(arrays_overlap(array_map(x -> split_part(x, '_', 1), c.acmg_criteria), ${pp4_blocked_by}),
            0, coalesce(p.pp4_points, 0)) AS pp4_points,
        CASE
            WHEN arrays_overlap(om.inheritance_code, ${recessive})
                AND NOT arrays_overlap(om.inheritance_code, ${dominant})
                AND g.zygosity = 'HET' AND sa.locus_id IS NULL THEN ${single_het_recessive_points}
            WHEN arrays_overlap(om.inheritance_code, ${dominant})
                AND NOT arrays_overlap(om.inheritance_code, ${recessive})
                AND g.healthy_carrier_parent THEN ${healthy_parent_dominant_points}
            ELSE 0
        END AS inheritance_points,
        c.acmg_points
    FROM ${output_db}.acmg_classification c
    JOIN ${shared_db}.snv__consequence k ON k.locus_id = c.locus_id AND k.transcript_id = c.transcript_id
    LEFT JOIN omim om ON om.symbol = k.symbol
    LEFT JOIN ${output_db}.acmg_phenotype p ON p.seq_id = c.seq_id AND p.symbol = k.symbol
    LEFT JOIN genotype g ON g.seq_id = c.seq_id AND g.locus_id = c.locus_id
    LEFT JOIN second_allele sa ON sa.seq_id = c.seq_id AND sa.locus_id = c.locus_id
    WHERE c.seq_id IN (${cases})
)
SELECT
    seq_id, locus_id, transcript_id, pp4_points, inheritance_points, points,
    pow(${very_strong_odds}, points / 8.0) * ${prior} / ((pow(${very_strong_odds}, points / 8.0) - 1) * ${prior} + 1),
    '${ruleset_version}'
FROM (SELECT *, acmg_points + pp4_points + inheritance_points AS points FROM scored) t;
