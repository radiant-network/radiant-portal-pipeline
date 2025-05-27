INSERT OVERWRITE {{ params.starrocks_variant_frequency }}
WITH freq AS  (
    SELECT
        locus_id,
        SUM(pc) AS pc,
        SUM(pn) AS pn
    FROM {{ params.starrocks_staging_variant_frequency }}
    GROUP BY locus_id
)
SELECT
    locus_id,
    pc,
    pn,
    pc / pn AS pf
FROM freq
;