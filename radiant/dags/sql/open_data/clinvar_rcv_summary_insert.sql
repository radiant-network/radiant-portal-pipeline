-- The RCV export spells clinical significance with spaces and commas ("Likely pathogenic, low penetrance");
-- rewrite it to the underscore form of clinvar.interpretations (ClinVar VCF CLNSIG), so both share dict_clinvar_interpretation.
INSERT OVERWRITE {{ mapping.starrocks_clinvar_rcv_summary }}
SELECT
    c.locus_id,
    s.clinvar_id,
    s.accession,
    array_map((cs) -> regexp_replace(cs, '[ ,]+', '_'), s.clinical_significance) AS clinical_significance,
    s.date_last_evaluated,
    s.submission_count,
    s.review_status,
    s.review_status_stars,
    s.version,
    s.traits,
    s.origins,
    s.submissions,
    s.clinical_significance_count
FROM {{ mapping.starrocks_raw_clinvar_rcv_summary }} s
JOIN (
    SELECT name, locus_id FROM {{ mapping.starrocks_clinvar }}
) c
ON c.name = s.clinvar_id;