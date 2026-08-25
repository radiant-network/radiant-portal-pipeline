-- One row per (germline case, family member) for the requested case ids, with the
-- member's sample, sequencing experiment and gVCF.
--
-- Consumed by `radiant-nextflow-postprocessing-cases` (resolve_cases).
--
-- The case ids alone scope the result. The clinical tables are a single shared schema
-- behind the `radiant_jdbc` catalog -- not one schema per tenant -- and `cases.id` is a
-- plain single-column identity primary key over all of it (`case_pkey`), with
-- `tenant_code` only an attribute. A case id therefore already names its tenant, which is
-- why the tenant is returned here rather than filtered on: a tenant taken as input could
-- only disagree with the data, and a correct case named under the wrong one would come
-- back as silence.
WITH gvcf_doc AS (
    SELECT tc.sequencing_experiment_id AS seq_id,
           d.url
    FROM {{ mapping.clinical_task }} t
    JOIN {{ mapping.clinical_task_context }}      tc  ON tc.task_id = t.id
    JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = t.id
                                                     AND thd.type = 'output'
    -- The gVCF is identified on the document's own type fields, never on its filename:
    -- naming conventions differ between callers. The germline code is `snv` (`ssnv` is
    -- somatic); the `data_type` dictionary has no `gsnv`.
    JOIN {{ mapping.clinical_document }}          d   ON d.id = thd.document_id
                                                     AND d.data_type_code = 'snv'
                                                     AND d.format_code    = 'gvcf'
    WHERE t.task_type_code = 'alignment_germline_variant_calling'
)
SELECT c.id                           AS case_id,
       c.submitter_case_id,
       c.primary_condition,
       c.tenant_code,
       pr.code                        AS project_code,
       f.relationship_to_proband_code AS role,
       f.affected_status_code         AS affected_status,
       p.id                           AS patient_id,
       p.sex_code                     AS sex,
       p.submitter_patient_id,
       s.submitter_sample_id          AS sample_id,
       se.id                          AS seq_id,
       se.aliquot,
       se.experimental_strategy_code  AS strategy,
       MAX(g.url)                     AS gvcf_url,
       COUNT(DISTINCT g.url)          AS gvcf_matches
FROM {{ mapping.clinical_case }} c
JOIN {{ mapping.clinical_family }}                          f    ON f.case_id = c.id
JOIN {{ mapping.clinical_patient }}                         p    ON p.id = f.family_member_id
JOIN {{ mapping.clinical_case_has_sequencing_experiment }}  chse ON chse.case_id = c.id
JOIN {{ mapping.clinical_sequencing_experiment }}           se   ON se.id = chse.sequencing_experiment_id
JOIN {{ mapping.clinical_sample }}                          s    ON s.id = se.sample_id
                                                                AND s.patient_id = p.id
-- On the primary key alone. `cases.project_id` is a foreign key to `project.id`, so this
-- resolves to exactly one row; `project.code` is globally unique, and a project row carries
-- a single tenant_code that need not equal the case's. Adding `AND pr.tenant_code =
-- c.tenant_code` therefore cannot narrow a genuine ambiguity -- there is none -- and can
-- only turn a match into a NULL. project_code is what the batch PATCH looks a case up by,
-- together with submitter_case_id, and it is mandatory there.
LEFT JOIN {{ mapping.clinical_project }}                    pr   ON pr.id = c.project_id
LEFT JOIN gvcf_doc g ON g.seq_id = se.id
WHERE c.id IN %(case_ids)s
  AND c.case_type_code = 'germline'
GROUP BY c.id, c.submitter_case_id, c.primary_condition, c.tenant_code, pr.code,
         f.relationship_to_proband_code, f.affected_status_code,
         p.id, p.sex_code, p.submitter_patient_id,
         s.submitter_sample_id, se.id, se.aliquot, se.experimental_strategy_code
ORDER BY c.id,
         CASE f.relationship_to_proband_code
              WHEN 'proband' THEN 0
              WHEN 'father'  THEN 1
              WHEN 'mother'  THEN 2
              ELSE 3
         END,
         p.id
