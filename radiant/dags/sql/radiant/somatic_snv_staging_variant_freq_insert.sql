INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ mapping.starrocks_somatic_snv_staging_variant_frequency }}
WITH
-- The tumor-only / tumor-normal verdict is a property of the task's complete aliquot set, so it is
-- computed at task_id grain and deliberately NOT filtered on `part`. The staging table is keyed
-- (case_id, seq_id, task_id), so one task spans several case_ids, and its rows only share a `part`
-- when tumor and normal agree on patient_id and experimental_strategy (part is assigned per
-- strategy — see radiant/tasks/starrocks/partition.py). Any narrower grouping hands back
-- half-tasks, and a half-task has one tumoral aliquot and no normal, so it would read as
-- tumor-only.
somatic_task_kinds AS (
    SELECT
        task_id,
        COUNT(DISTINCT CASE WHEN histology_type = 'tumoral' THEN aliquot END) AS n_tumoral,
        COUNT(DISTINCT CASE WHEN histology_type = 'normal'  THEN aliquot END) AS n_normal
    FROM {{ mapping.starrocks_staging_sequencing_experiment }}
    WHERE analysis_type = 'somatic'
      AND tenant_code = %(tenant_code)s
    GROUP BY task_id
),
-- One row per (task, patient, strategy) in this part, carrying the task-level verdict. The two
-- flags are independent on purpose: `NOT is_tumor_only` would count a malformed normal-only task
-- (0 tumoral / 1 normal) or a multi-region tumor task (2 tumoral / 0 normal) in the tumor-normal
-- cohort, which the `tumoral > 0 AND normal > 0` predicate excludes. Malformed tasks belong to
-- neither cohort.
somatic_tasks AS (
    SELECT DISTINCT
        s.task_id,
        s.part,
        s.patient_id,
        s.experimental_strategy,
        k.n_tumoral = 1 AND k.n_normal = 0 AS is_tumor_only,
        k.n_tumoral > 0 AND k.n_normal > 0 AS is_tumor_normal
    FROM {{ mapping.starrocks_staging_sequencing_experiment }} s
    JOIN somatic_task_kinds k ON k.task_id = s.task_id
    WHERE s.analysis_type = 'somatic'
      AND s.tenant_code = %(tenant_code)s
      AND s.part = %(part)s
),
-- Denominators come from the tasks, never from the occurrences: a patient with a tumor-only task
-- and zero qualifying loci in this partition still belongs in pn_to_*. A patient analysed both
-- ways counts in both cohorts, which is intended.
patients_total_count_cohort AS (
    SELECT
        part,
        COUNT(DISTINCT CASE WHEN is_tumor_normal AND experimental_strategy = 'wgs' THEN patient_id END) AS cnt_tn_wgs,
        COUNT(DISTINCT CASE WHEN is_tumor_normal AND experimental_strategy = 'wxs' THEN patient_id END) AS cnt_tn_wxs,
        COUNT(DISTINCT CASE WHEN is_tumor_only   AND experimental_strategy = 'wgs' THEN patient_id END) AS cnt_to_wgs,
        COUNT(DISTINCT CASE WHEN is_tumor_only   AND experimental_strategy = 'wxs' THEN patient_id END) AS cnt_to_wxs
    FROM somatic_tasks
    GROUP BY part
),
freqs_tumor AS (
    SELECT
        o.part,
        o.locus_id,
        COUNT(DISTINCT CASE WHEN t.is_tumor_normal AND t.experimental_strategy = 'wgs' THEN t.patient_id END) AS pc_tn_wgs,
        COUNT(DISTINCT CASE WHEN t.is_tumor_normal AND t.experimental_strategy = 'wxs' THEN t.patient_id END) AS pc_tn_wxs,
        COUNT(DISTINCT CASE WHEN t.is_tumor_only   AND t.experimental_strategy = 'wgs' THEN t.patient_id END) AS pc_to_wgs,
        COUNT(DISTINCT CASE WHEN t.is_tumor_only   AND t.experimental_strategy = 'wxs' THEN t.patient_id END) AS pc_to_wxs
    FROM {{ mapping.starrocks_somatic_snv_occurrence }} o
    -- task_id, NOT seq_id: one tumor sample can be analysed both tumor-only and tumor-normal, so
    -- joining `s.seq_id = o.tumor_seq_id` duplicated the occurrence once per task using that
    -- sample and made the tumor-only / tumor-normal split impossible. Tenant isolation comes from
    -- somatic_tasks (tenant-filtered) plus the per-tenant occurrence database — the occurrence
    -- table has no tenant_code column of its own.
    JOIN somatic_tasks t ON t.task_id = o.task_id
    WHERE o.part = %(part)s
      AND o.filter = 'PASS'
      AND o.tumor_ad_alt > 2
    GROUP BY o.locus_id, o.part
)
SELECT
    %(tenant_code)s AS tenant_code,
    part,
    locus_id,
    pc_tn_wgs,
    (SELECT cnt_tn_wgs FROM patients_total_count_cohort)                          AS pn_tn_wgs,
    pc_tn_wgs / NULLIF((SELECT cnt_tn_wgs FROM patients_total_count_cohort), 0)   AS pf_tn_wgs,
    pc_tn_wxs,
    (SELECT cnt_tn_wxs FROM patients_total_count_cohort)                          AS pn_tn_wxs,
    pc_tn_wxs / NULLIF((SELECT cnt_tn_wxs FROM patients_total_count_cohort), 0)   AS pf_tn_wxs,
    pc_to_wgs,
    (SELECT cnt_to_wgs FROM patients_total_count_cohort)                          AS pn_to_wgs,
    pc_to_wgs / NULLIF((SELECT cnt_to_wgs FROM patients_total_count_cohort), 0)   AS pf_to_wgs,
    pc_to_wxs,
    (SELECT cnt_to_wxs FROM patients_total_count_cohort)                          AS pn_to_wxs,
    pc_to_wxs / NULLIF((SELECT cnt_to_wxs FROM patients_total_count_cohort), 0)   AS pf_to_wxs
FROM freqs_tumor
