INSERT OVERWRITE {{ mapping.starrocks_gnomad_constraint }}
SELECT
	t.transcript as transcript_id,
	t.pLI as pli,
    t.oe_lof_upper as loeuf
FROM {{ mapping.iceberg_gnomad_constraint }} t

