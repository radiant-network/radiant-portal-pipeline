-- One row per case the pipeline may move: every `submitted` or `processing` case, its tenant,
-- and whether it has variant data.
--
-- Consumed by `radiant-case-status-control` twice: at the start (`fetch_case_statuses`, for
-- `submitted` -> `processing`) and after the pipelines (`refresh_case_statuses`, for
-- `processing` -> `in_progress`). Unscoped on purpose: a case whose pipeline failed on an
-- earlier run must still be found once its variants are in.
--
-- `has_variants`: at least one experiment of the case has been imported (`ingested_at`, set by
-- `import_part`) from an SNV or CNV VCF, and is not deleted. An Exomiser-only row does not count.
WITH imported AS (
    SELECT DISTINCT se.case_id AS case_id
    FROM {{ mapping.starrocks_staging_sequencing_experiment }} se
    WHERE se.ingested_at IS NOT NULL
      AND NOT se.deleted
      AND (se.vcf_filepath IS NOT NULL OR se.cnv_vcf_filepath IS NOT NULL)
)
SELECT c.id                  AS case_id,
       c.tenant_code         AS tenant_code,
       c.status_code         AS status_code,
       i.case_id IS NOT NULL AS has_variants
FROM {{ mapping.clinical_case }} c
LEFT JOIN imported i ON i.case_id = c.id
WHERE c.status_code IN ('submitted', 'processing')
ORDER BY c.id
