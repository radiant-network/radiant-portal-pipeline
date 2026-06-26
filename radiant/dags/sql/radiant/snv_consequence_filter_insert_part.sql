INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ mapping.starrocks_snv_consequence_filter_partitioned }}
SELECT
    %(part)s AS part,
    c.*
FROM {{ mapping.starrocks_snv_consequence_filter }} c
LEFT SEMI JOIN (
    -- We need to bring all the locus_id(s) from each tenant's occurrences back to the consequence table
    {% for t in tenants %}
    SELECT locus_id
    FROM {{ per_tenant_mapping(t).starrocks_germline_snv_occurrence }}
    WHERE part in (%(part)s)
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
) o ON o.locus_id = c.locus_id;
