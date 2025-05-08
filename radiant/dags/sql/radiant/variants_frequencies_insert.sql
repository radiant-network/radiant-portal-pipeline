INSERT OVERWRITE {{ params.starrocks_variants_frequencies }}
WITH freq AS  (
    SELECT
        locus_id,
        SUM(pc) AS pc,
        SUM(pn) AS an
    FROM {{ params.starrocks_staging_variants_frequencies }}
    GROUP BY locus_id
)

SELECT
    locus_id,
    freq.pc,
    freq.an,
    freq.pc / freq.an AS pf
FROM freq