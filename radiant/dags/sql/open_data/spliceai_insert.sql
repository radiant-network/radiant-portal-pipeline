INSERT OVERWRITE {{ mapping.starrocks_spliceai }}
SELECT
    COALESCE(GET_VARIANT_ID(s.chromosome, s.start, s.reference, s.alternate), v.locus_id) as locus_id,
    s.symbol,
    s.max_score.ds AS spliceai_ds,
    s.max_score.type AS spliceai_type
FROM {{ mapping.iceberg_spliceai }} s
LEFT JOIN {{ mapping.starrocks_variant_lookup }} v ON v.locus_hash = {% if mapping.iceberg_spliceai_is_contract %}sha2(concat_ws('-', s.chromosome, s.start, s.reference, s.alternate), 256){% else %}s.locus_hash{% endif %}
WHERE s.chromosome in
    ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20',
    '21', '22', 'X', 'Y', 'M')
