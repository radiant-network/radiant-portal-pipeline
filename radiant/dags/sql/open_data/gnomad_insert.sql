-- Source is OpenDataLake's `gnomad_joint_v1` (gnomAD v4.1 joint callset), which suffixes every frequency
-- column by callset and publishes no unsuffixed one. The `joint` callset is gnomAD's recommended default.
-- The StarRocks target keeps its historical `gnomad_genomes_v3` name and unsuffixed columns
-- (design/SJRA-1811-opendatalake-integration.md, decision on the callset).
-- OpenDataLake publishes no `locus_hash`: recomputed here, byte-identical to the VCF ingest
-- path (radiant/tasks/vcf/snv/common.py) and to raw_exomiser's generated column.
INSERT OVERWRITE {{ mapping.starrocks_gnomad_genomes_v3 }}
SELECT
    COALESCE(GET_VARIANT_ID(t.chromosome, t.start, t.reference, t.alternate), v.locus_id) as locus_id,
    t.af_joint AS af,
    t.ac_joint AS ac,
    t.an_joint AS an,
    t.hom_joint AS nhomalt
FROM {{ mapping.iceberg_gnomad_joint }} t
LEFT JOIN {{ mapping.starrocks_variant_lookup }} v ON v.locus_hash = sha2(concat_ws('-', t.chromosome, t.start, t.reference, t.alternate), 256)
