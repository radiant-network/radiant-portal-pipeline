-- Named partition, as in germline_snv_staging_variant_freq_insert.sql.
INSERT OVERWRITE {{ mapping.starrocks_snv_consequence_filter_partitioned }} PARTITION (part = %(part)s)
SELECT
    %(part)s AS part,
    c.*
FROM {{ mapping.starrocks_snv_consequence_filter }} c
LEFT SEMI JOIN (
    {% for t in tenants %}
    SELECT locus_id
    FROM {{ per_tenant_mapping(t).starrocks_germline_snv_occurrence }}
    WHERE part in (%(part)s)
    UNION ALL
    SELECT locus_id
    FROM {{ per_tenant_mapping(t).starrocks_somatic_snv_occurrence }}
    WHERE part in (%(part)s)
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
) o ON o.locus_id = c.locus_id;
