INSERT INTO {{ params.starrocks_variant_lookup }}(`locus_hash`)
SELECT
    `locus_hash`
FROM {{ params.iceberg_spliceai }} s
LEFT ANTI JOIN {{ params.starrocks_variant_lookup }} vd ON vd.locus_hash=s.locus_hash
WHERE GET_VARIANT_ID(s.chromosome, s.start, s.reference, s.alternate) IS NULL
AND s.chromosome in
    ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20',
    '21', '22', 'X', 'Y', 'M');