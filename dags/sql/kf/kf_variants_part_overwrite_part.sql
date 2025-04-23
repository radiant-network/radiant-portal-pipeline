INSERT OVERWRITE variants_part PARTITION (p{part_id})
SELECT
    {part_id} AS part,
    v.*
FROM
    variants v
LEFT SEMI JOIN occurrences o
ON v.locus_id = o.locus_id AND o.part >= {part_lower} AND o.part < {part_upper};
