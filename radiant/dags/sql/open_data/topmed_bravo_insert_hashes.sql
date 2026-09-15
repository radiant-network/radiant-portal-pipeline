-- OpenDataLake publishes no `locus_hash` (dbnsfp excepted), so the contract side recomputes it --
-- byte-identical to the VCF ingest path (radiant/tasks/vcf/snv/common.py) and to raw_exomiser's generated
-- column. The pre-contract table stores the column, so a source held back via
-- RADIANT_OPEN_DATA_USE_LEGACY_TABLES reads it instead of paying a SHA-256 over every row.
-- Either way it is computed once, inside the subquery, so the join key and the projected column share a
-- single evaluation. The GET_VARIANT_ID filter sits in there too, so only variants that need a surrogate
-- id are considered at all.
INSERT INTO {{ mapping.starrocks_variant_lookup }}(`locus_hash`)
SELECT h.locus_hash
FROM (
    SELECT {% if mapping.iceberg_topmed_bravo_is_contract %}
               sha2(concat_ws('-', src.chromosome, src.start, src.reference, src.alternate), 256)
           {% else %}
               src.locus_hash
           {% endif %} AS locus_hash
    FROM {{ mapping.iceberg_topmed_bravo }} src
    WHERE GET_VARIANT_ID(src.chromosome, src.start, src.reference, src.alternate) IS NULL
) h
LEFT ANTI JOIN {{ mapping.starrocks_variant_lookup }} vd ON vd.locus_hash = h.locus_hash;
