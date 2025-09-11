INSERT OVERWRITE {{ mapping.starrocks_clinvar_rcv_summary }}
SELECT
    c.locus_id,
    s.*
FROM {{ mapping.starrocks_raw_clinvar_rcv_summary }} s
JOIN (
    SELECT name, locus_id FROM {{ mapping.starrocks_clinvar }}
) c
ON c.name = s.clinvar_id;