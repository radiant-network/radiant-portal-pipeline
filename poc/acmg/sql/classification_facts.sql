-- Classification facts: one row per occurrence of the cases asked for on a classified (locus, transcript), with
-- the criteria met by its variant and by the occurrence itself (occurrences with none get an empty list), and the
-- criteria the ClinGen VCEPs of the transcript's gene declare not applicable.
WITH classified AS (
    ${classified}
),
occurrence AS (
    SELECT DISTINCT o.seq_id, o.locus_id
    FROM ${tenant_db}.germline__snv__occurrence o
    WHERE o.seq_id IN (${cases})
        ${occurrence_part}
        AND o.locus_id IN (SELECT locus_id FROM classified)
),
evidence AS (
    SELECT o.seq_id, o.locus_id, k.transcript_id, vc.criterion, vc.code, vc.strength, vc.points, vc.value, vc.threshold
    FROM occurrence o
    JOIN classified k ON k.locus_id = o.locus_id
    JOIN ${output_db}.acmg_variant_criteria vc ON vc.locus_id = k.locus_id AND vc.transcript_id = k.transcript_id
    UNION ALL
    SELECT o.seq_id, o.locus_id, k.transcript_id, oc.criterion, oc.code, oc.strength, oc.points, oc.value, oc.threshold
    FROM occurrence o
    JOIN classified k ON k.locus_id = o.locus_id
    JOIN ${output_db}.acmg_occurrence_criteria oc ON oc.seq_id = o.seq_id AND oc.locus_id = o.locus_id
),
not_applicable AS (
    SELECT k.locus_id, k.transcript_id, array_agg(n.criterion) AS criteria
    FROM classified k
    JOIN ${shared_db}.snv__consequence c ON c.locus_id = k.locus_id AND c.transcript_id = k.transcript_id
    LEFT JOIN ${output_db}.gene_symbol gs ON gs.symbol = c.symbol
    JOIN ${output_db}.cspec_not_applicable n ON n.symbol = coalesce(gs.current, c.symbol)
    GROUP BY k.locus_id, k.transcript_id
),
grouped AS (
    SELECT
        CAST(o.seq_id AS BIGINT) AS seq_id,
        o.locus_id,
        k.transcript_id,
        array_agg(named_struct(
            'criterion', e.criterion,
            'code', e.code,
            'strength', e.strength,
            'points', CAST(e.points AS BIGINT),
            'value', e.value,
            'threshold', e.threshold
        )) AS criteria
    FROM occurrence o
    JOIN classified k ON k.locus_id = o.locus_id
    LEFT JOIN evidence e ON e.seq_id = o.seq_id AND e.locus_id = o.locus_id AND e.transcript_id = k.transcript_id
    GROUP BY o.seq_id, o.locus_id, k.transcript_id
)
SELECT g.seq_id, g.locus_id, g.transcript_id, g.criteria,
    coalesce(na.criteria, CAST([] AS ARRAY<VARCHAR(10)>)) AS not_applicable
FROM grouped g
LEFT JOIN not_applicable na ON na.locus_id = g.locus_id AND na.transcript_id = g.transcript_id
