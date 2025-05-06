INSERT OVERWRITE {{ params.starrocks_variants_frequencies }}
SELECT
    locus_id,
    SUM(pc) AS pc,
    SUM(ac) AS ac,
    SUM(hom) AS hom
FROM {{ params.starrocks_staging_variants_frequencies }}
GROUP BY locus_id;
