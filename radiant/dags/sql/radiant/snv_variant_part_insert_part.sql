-- Named partition, as in germline_snv_staging_variant_freq_insert.sql.
INSERT OVERWRITE {{ mapping.starrocks_snv_variant_partitioned }} PARTITION (part = %(variant_part)s)
SELECT
    %(variant_part)s AS part,
    v.*
FROM
    {{ mapping.starrocks_snv_variant }} v
LEFT SEMI JOIN (
    SELECT locus_id
    FROM {{ mapping.starrocks_germline_snv_occurrence }}
    WHERE part >= %(part_lower)s AND part < %(part_upper)s
    UNION ALL
    SELECT locus_id
    FROM {{ mapping.starrocks_somatic_snv_occurrence }}
    WHERE part >= %(part_lower)s AND part < %(part_upper)s
) o ON v.locus_id = o.locus_id;
