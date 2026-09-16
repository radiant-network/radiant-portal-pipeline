INSERT OVERWRITE {{ mapping.starrocks_gnomad_genomes_v3 }}
SELECT
    COALESCE(GET_VARIANT_ID(t.chromosome, t.start, t.reference, t.alternate), v.locus_id) as locus_id,
{% if mapping.iceberg_gnomad_joint_is_contract %}
    t.af_joint AS af,
    t.ac_joint AS ac,
    t.an_joint AS an,
    t.hom_joint AS nhomalt
{% else %}
    t.af,
    t.ac,
    t.an,
    t.nhomalt
{% endif %}
FROM {{ mapping.iceberg_gnomad_joint }} t
LEFT JOIN {{ mapping.starrocks_variant_lookup }} v ON v.locus_hash = t.locus_hash
