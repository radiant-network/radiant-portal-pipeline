-- RAD-287. Who is in a pipeline case and in which role: one row per `family` row of the case. It does not
-- depend on sequencing: a member is listed before any of their experiments is complete.
--
-- The portal writes one `family` row for every patient of a case, proband included, so `family.id`
-- (`family_row_id`) identifies a (case, member) pair, not a family. A somatic case can still have no row for
-- its proband: consumers that must not lose such a proband LEFT JOIN this view.
CREATE VIEW IF NOT EXISTS {{ mapping.starrocks_case_member }} AS
SELECT f.case_id                      AS case_id,
       f.family_member_id             AS patient_id,
       f.id                           AS family_row_id,
       f.relationship_to_proband_code AS role,
       f.affected_status_code         AS affected_status,
       p.sex_code                     AS sex,
       p.submitter_patient_id         AS submitter_patient_id
FROM {{ mapping.clinical_family }} f
JOIN {{ mapping.starrocks_pipeline_case }} pc ON pc.case_id = f.case_id
JOIN {{ mapping.clinical_patient }} p         ON p.id = f.family_member_id
