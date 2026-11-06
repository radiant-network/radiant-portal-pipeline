INSERT OVERWRITE {{ mapping.starrocks_gnomad_constraint }}
SELECT
	t.transcript as transcript_id,
{% if mapping.iceberg_gnomad_constraint_is_contract %}
	-- gnomAD v4.1.2 constraint_metrics (`gnomad_constraint_v1`): `lof` is the high-confidence pLoF category,
	-- the same one v2.1.1 published unprefixed. LOEUF is the upper bound of its o/e confidence interval.
	t.lof_pLI as pli,
	t.lof_oe_ci_upper as loeuf
{% else %}
	-- gnomAD v2.1.1 lof_metrics.by_gene (pre-contract `gnomad_constraint_v_2_1_1`).
	t.pLI as pli,
	t.oe_lof_upper as loeuf
{% endif %}
FROM {{ mapping.iceberg_gnomad_constraint }} t
