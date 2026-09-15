-- Reads OpenDataLake's `gnomad_joint_v1` (gnomAD v4.1 joint callset), which suffixes every frequency
-- column by callset and publishes no unsuffixed one. The `joint` callset is gnomAD's recommended
-- default. The StarRocks target keeps its historical `gnomad_genomes_v3` name and unsuffixed columns
-- (design/SJRA-1811-opendatalake-integration.md, decision on the callset).
--
-- The branch below is the only place a source's column *names* differ between the two sides, so it is
-- the only statement that has to know which one it is reading. It matters when
-- RADIANT_OPEN_DATA_CONTRACT_TABLES leaves `gnomad_joint` out and the source falls back to the
-- pre-contract `gnomad_genomes_v3`, whose columns are already unsuffixed.
--
-- OpenDataLake publishes no `locus_hash`: recomputed here, byte-identical to the VCF ingest
-- path (radiant/tasks/vcf/snv/common.py) and to raw_exomiser's generated column. The recomputation is
-- correct on both sides -- the pre-contract table stores the same hash over the same four columns.
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
LEFT JOIN {{ mapping.starrocks_variant_lookup }} v ON v.locus_hash = {% if mapping.iceberg_gnomad_joint_is_contract %}sha2(concat_ws('-', t.chromosome, t.start, t.reference, t.alternate), 256){% else %}t.locus_hash{% endif %}
