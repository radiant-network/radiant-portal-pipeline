INSERT INTO {{ mapping.starrocks_variant_lookup }}(`locus_hash`)
SELECT src.locus_hash
FROM {{ mapping.iceberg_spliceai }} src
LEFT ANTI JOIN {{ mapping.starrocks_variant_lookup }} vd ON vd.locus_hash = src.locus_hash
WHERE GET_VARIANT_ID(src.chromosome, src.start, src.reference, src.alternate) IS NULL
  AND src.chromosome in
    ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18',
    '19', '20', '21', '22', 'X', 'Y', 'M');
