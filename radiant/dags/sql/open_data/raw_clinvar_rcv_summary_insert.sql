-- The OpenDataLake-backed load of the raw ClinVar RCV summary.
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
