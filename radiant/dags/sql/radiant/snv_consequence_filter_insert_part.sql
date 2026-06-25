INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ mapping.starrocks_snv_consequence_filter_partitioned }}
SELECT
    %(part)s AS part,
    c.*
FROM {{ mapping.starrocks_snv_consequence_filter }} c
LEFT SEMI JOIN (
    -- Occurrences live in per-tenant databases; union them so the shared partitioned consequence filter
    -- keeps the loci present for this part across all tenants.
    {% for t in tenants %}
    SELECT locus_id
    FROM {{ per_tenant_mapping(t).starrocks_germline_snv_occurrence }}
    WHERE part in (%(part)s)
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
) o ON o.locus_id = c.locus_id;
