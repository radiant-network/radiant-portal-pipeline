INSERT OVERWRITE {{ params.starrocks_clinvar_rcv_summary }}
SELECT
    c.locus_id,
    s.*
FROM {{ params.starrocks_raw_clinvar_rcv_summary }} s
JOIN (
    SELECT name, locus_id FROM {{ params.starrocks_clinvar }}
) c
ON c.name = s.clinvar_id;