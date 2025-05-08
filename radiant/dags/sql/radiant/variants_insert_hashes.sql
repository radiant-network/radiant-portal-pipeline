INSERT INTO {{ params.starrocks_variants_lookup  }} (`locus_hash`)
SELECT
    distinct `locus_hash`
FROM {{ params.iceberg_variants }} v
where v.case_id in %(case_ids)s
LEFT ANTI JOIN {{ params.starrocks_variants_lookup }} vd ON vd.locus_hash=v.locus_hash;