INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ mapping.starrocks_snv_variant_partitioned }}
SELECT
    %(variant_part)s AS part,
    v.*
FROM
    {{ mapping.starrocks_snv_variant }} v
LEFT SEMI JOIN (
    -- Loop through all tenants to get the data for this partition
    {% for t in tenants %}
    SELECT locus_id
    FROM {{ per_tenant_mapping(t).starrocks_germline_snv_occurrence }}
    WHERE part >= %(part_lower)s AND part < %(part_upper)s
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
) o ON v.locus_id = o.locus_id;
