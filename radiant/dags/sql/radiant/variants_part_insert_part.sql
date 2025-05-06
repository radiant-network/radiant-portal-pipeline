INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ params.starrocks_variants_partitioned }}
SELECT
    %(variant_part)s AS part,
    v.*
FROM
    {{ params.starrocks_variants }} v
LEFT SEMI JOIN {{ params.starrocks_occurrences }} o
ON v.locus_id = o.locus_id AND o.part >= %(part_lower)s AND o.part < %(part_upper)s;
