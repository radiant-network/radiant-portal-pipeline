INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ mapping.starrocks_germline_snv_variant_partitioned }}
SELECT
    %(variant_part)s AS part,
    v.*
FROM
    {{ mapping.starrocks_germline_snv_variant }} v
LEFT SEMI JOIN {{ mapping.starrocks_germline_snv_occurrence }} o
ON v.locus_id = o.locus_id AND o.part >= %(part_lower)s AND o.part < %(part_upper)s;
