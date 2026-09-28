-- The OpenDataLake-backed load of the raw ClinVar RCV summary. `clinvar_rcv_v1` publishes exactly
-- the columns this table holds, in the same types, so the copy is a straight projection --
-- `locus_id` is added afterwards by `clinvar_rcv_summary_insert.sql`, joining `clinvar`.
--
-- Guarded: held back, `clinvar_rcv` has no `mapping.iceberg_clinvar_rcv` to read at all, and the
-- table is filled by the broker load instead (`raw_clinvar_rcv_summary_load.sql`).
{% if mapping.iceberg_clinvar_rcv_is_contract %}
INSERT OVERWRITE {{ mapping.starrocks_raw_clinvar_rcv_summary }}
SELECT
    c.clinvar_id,
    c.accession,
    c.clinical_significance,
    c.date_last_evaluated,
    c.submission_count,
    c.review_status,
    c.review_status_stars,
    c.version,
    c.traits,
    c.origins,
    c.submissions,
    c.clinical_significance_count
FROM {{ mapping.iceberg_clinvar_rcv }} c
{% endif %}
