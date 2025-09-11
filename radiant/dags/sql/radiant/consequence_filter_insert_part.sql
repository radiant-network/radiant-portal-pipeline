INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ mapping.starrocks_consequence_filter_partitioned }}
SELECT
    %(part)s AS part,
    c.*
FROM {{ mapping.starrocks_consequence_filter }} c
LEFT SEMI JOIN {{ mapping.starrocks_occurrence }} o ON o.locus_id = c.locus_id AND o.part in (%(part)s)