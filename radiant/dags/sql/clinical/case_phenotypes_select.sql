-- HPO terms per (case, patient) for the requested case ids.
--
-- Consumed by `radiant-nextflow-postprocessing-cases` (resolve_cases) to build the
-- proband's phenopacket. `interpretation_code = 'negative'` means the term was explicitly
-- excluded, so the writer emits it as `excluded: true` rather than as an observed feature.
--
-- No tenant filter: `case_id` is globally unique, so it already scopes the result. See
-- `case_members_select.sql`.
--
-- The join crosses catalogs on purpose: obs_categorical is clinical (radiant_jdbc) while
-- hpo_term is a shared open-data dictionary in the base StarRocks database. It is a LEFT
-- JOIN so an HPO code the dictionary does not know yields a null label instead of dropping
-- the row -- a missing label is cosmetic, a missing phenotype is not.
SELECT oc.case_id,
       oc.patient_id,
       oc.code_value          AS hpo_id,
       h.name                 AS hpo_label,
       oc.onset_code,
       oc.interpretation_code
FROM {{ mapping.clinical_obs_categorical }} oc
LEFT JOIN {{ mapping.starrocks_hpo_term }} h ON h.id = oc.code_value
WHERE oc.case_id IN %(case_ids)s
  AND oc.observation_code = 'phenotype'
  AND oc.coding_system    = 'HPO'
ORDER BY oc.case_id, oc.patient_id, oc.code_value
