-- RAD-287. For each sequencing experiment, the alignment and variant calling result that counts today: the
-- supersession policy one level down (SJRA-1698 5). A re-aligned experiment carries two alignment tasks; the
-- newest wins. Germline and somatic calling are two products, ranked apart: consumers filter on `task_type`.
--
-- It belongs to the experiment, not to a case: an experiment shared by two cases has one current alignment.
-- Joined through `sequencing_experiment_id`, never `task_context.case_id`: alignment tasks carry a null
-- case_id, so joining on it returns an empty set, not an error.
CREATE VIEW IF NOT EXISTS {{ mapping.starrocks_current_alignment }} AS
SELECT seq_id, task_type, task_id, pipeline_name
FROM (
    SELECT tc.sequencing_experiment_id AS seq_id,
           t.task_type_code            AS task_type,
           t.id                        AS task_id,
           t.pipeline_name             AS pipeline_name,
           ROW_NUMBER() OVER (
               PARTITION BY tc.sequencing_experiment_id, t.task_type_code
               ORDER BY t.created_on DESC, t.id DESC
           ) AS rn
    FROM {{ mapping.clinical_task }} t
    JOIN {{ mapping.clinical_task_context }} tc ON tc.task_id = t.id
    WHERE t.task_type_code IN ('alignment_germline_variant_calling', 'alignment_somatic_variant_calling')
) ranked
WHERE rn = 1
