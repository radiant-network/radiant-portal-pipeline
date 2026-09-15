INSERT INTO {{ mapping.starrocks_variant_lookup }}(`locus_hash`)
SELECT h.locus_hash
FROM (
    SELECT {% if mapping.iceberg_1000_genomes_is_contract %}
               sha2(concat_ws('-', src.chromosome, src.start, src.reference, src.alternate), 256)
           {% else %}
               src.locus_hash
           {% endif %} AS locus_hash
    FROM {{ mapping.iceberg_1000_genomes }} src
    WHERE GET_VARIANT_ID(src.chromosome, src.start, src.reference, src.alternate) IS NULL
) h
LEFT ANTI JOIN {{ mapping.starrocks_variant_lookup }} vd ON vd.locus_hash = h.locus_hash;
