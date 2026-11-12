-- RAD-287. For each person of a pipeline case, the sequencing that counts today: the supersession policy
-- (SJRA-1698 5), one `completed` experiment per (case, patient), newest wins. A re-sequenced member otherwise
-- appears twice -- silently, when that member is a parent. `completed` is a whitelist: unfinished sequencing
-- is not a candidate.
--
-- A somatic case ranks per histology too: its proband has a tumor and a normal, and each keeps its own current
-- experiment. For a germline case the third key is NULL, so the partition is (case, patient). A consumer that
-- wants the proband's tumor filters `histology_code = 'tumoral'` on this view: the filter removes whole
-- partitions, so it gives the row that filtering before ranking gave.
CREATE VIEW IF NOT EXISTS {{ mapping.starrocks_current_experiment }} AS
SELECT case_id, case_type, patient_id, histology_code, seq_id, sample_id, aliquot, strategy
FROM (
    SELECT pc.case_id                    AS case_id,
           pc.case_type                  AS case_type,
           s.patient_id                  AS patient_id,
           s.histology_code              AS histology_code,
           se.id                         AS seq_id,
           s.submitter_sample_id         AS sample_id,
           se.aliquot                    AS aliquot,
           se.experimental_strategy_code AS strategy,
           ROW_NUMBER() OVER (
               PARTITION BY pc.case_id,
                            s.patient_id,
                            CASE WHEN pc.case_type = 'somatic' THEN s.histology_code END
               ORDER BY se.created_on DESC, se.id DESC
           ) AS rn
    FROM {{ mapping.clinical_case_has_sequencing_experiment }} chse
    JOIN {{ mapping.starrocks_pipeline_case }} pc              ON pc.case_id = chse.case_id
    JOIN {{ mapping.clinical_sequencing_experiment }} se       ON se.id = chse.sequencing_experiment_id
    JOIN {{ mapping.clinical_sample }} s                       ON s.id = se.sample_id
    WHERE se.status_code = 'completed'
) ranked
WHERE rn = 1
