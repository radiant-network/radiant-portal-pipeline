INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ mapping.starrocks_variant_partitioned }}
SELECT
    %(variant_part)s AS part,
    v.*
FROM
    {{ mapping.starrocks_variant }} v
LEFT SEMI JOIN {{ mapping.starrocks_occurrence }} o
ON v.locus_id = o.locus_id AND o.part >= %(part_lower)s AND o.part < %(part_upper)s;
