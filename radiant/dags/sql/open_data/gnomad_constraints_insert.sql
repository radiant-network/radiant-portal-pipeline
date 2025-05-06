INSERT OVERWRITE {{ params.starrocks_gnomad_constraints }}
SELECT
	t.transcript as transcript_id,
	t.pLI as pli,
    t.oe_lof_upper as loeuf
FROM {{ params.iceberg_gnomad_constraints }} t

