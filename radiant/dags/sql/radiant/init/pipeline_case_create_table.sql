-- RAD-287. The cases the pipelines may look at: one row per case whose status is not ignored. A draft or
-- revoked case does not exist for the ETL: a revoked case is often one left alone on purpose, and admitting
-- it would resurrect every one of them nightly. The case type decides which rules apply to it.
--
-- `pipeline_ignored_case_statuses` is rendered when the view is created: changing the list means
-- re-creating this view (drop it, re-run the Init StarRocks Base Tables DAG).
CREATE VIEW IF NOT EXISTS {{ mapping.starrocks_pipeline_case }} AS
SELECT c.id                AS case_id,
       c.case_type_code    AS case_type,
       c.status_code       AS status_code,
       c.tenant_code       AS tenant_code,
       c.submitter_case_id AS submitter_case_id,
       c.primary_condition AS primary_condition,
       c.project_id        AS project_id,
       c.proband_id        AS proband_id,
       c.priority_code     AS priority_code,
       pr.code             AS project_code
FROM {{ mapping.clinical_case }} c
-- On the primary key alone. `project.code` is globally unique and a project's tenant_code need not equal the
-- case's, so `AND pr.tenant_code = c.tenant_code` could only turn a match into a NULL. The batch PATCH looks a
-- case up by (project_code, submitter_case_id). NULL when unresolved: the pending queries report it.
LEFT JOIN {{ mapping.clinical_project }} pr ON pr.id = c.project_id
WHERE c.status_code NOT IN ({{ mapping.pipeline_ignored_case_statuses }})
