-- Classification facts: one row per occurrence of the cases asked for on a classified (locus, transcript), with
-- the criteria met by its variant and by the occurrence itself. Occurrences with none get an empty list.
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
)
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
