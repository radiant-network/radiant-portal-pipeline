INSERT INTO {{ params.starrocks_variants_partitioned }}
SELECT
    {part_id} AS part,
    v.*
FROM
    variants v
LEFT SEMI JOIN {{ params.starrocks_occurrences }} o
ON v.locus_id = o.locus_id AND o.part >= {part_lower} AND o.part < {part_upper};
