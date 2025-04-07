INSERT OVERWRITE kf_variants_part PARTITION (p{part_id})
SELECT
    {part_id} AS part,
    v.*
FROM
    kf_variants v
LEFT SEMI JOIN kf_occurrences o
ON v.locus_id = o.locus_id AND o.part >= {part_lower} AND o.part < {part_upper};
