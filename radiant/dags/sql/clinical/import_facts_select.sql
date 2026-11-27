-- The facts the import decides on (`radiant.tasks.importing.resolve`): one row per (case, seq, task) it has
-- work for, read by `radiant-import`.
--
-- `state` says where the row is:
-- - `new`: in importable_task, not yet in staging (anti-join on the key only: a task never changes). Carries the
--   parts its patient, experiment, case and family already have in staging, for the partition assigner;
-- - `staged`: in staging, not loaded yet (`ingested_at` at the epoch: inserted, its `import_part` failed), or
--   `deleted`. Rows staged before the key-only delta keep their old `updated_at`, hence the date comparison.
-- `deleted` is the flag, or a task the clinical model no longer has: the decision sets the flag.
WITH new_row AS (
    SELECT it.*
    FROM {{ mapping.starrocks_importable_task }} it
    LEFT ANTI JOIN {{ mapping.starrocks_staging_sequencing_experiment }} existing
    ON
        it.case_id = existing.case_id AND
        it.seq_id = existing.seq_id AND
        it.task_id = existing.task_id
),
patient_part AS (
    SELECT MAX(se.part) AS part, nr.patient_id, nr.experimental_strategy
    FROM new_row nr
    LEFT JOIN {{ mapping.starrocks_staging_sequencing_experiment }} se
    ON se.patient_id = nr.patient_id AND se.experimental_strategy = nr.experimental_strategy
    GROUP BY nr.patient_id, nr.experimental_strategy
),
seq_part AS (
    SELECT MAX(se.part) AS part, nr.seq_id, nr.experimental_strategy
    FROM new_row nr
    LEFT JOIN {{ mapping.starrocks_staging_sequencing_experiment }} se
    ON se.seq_id = nr.seq_id AND se.experimental_strategy = nr.experimental_strategy
    GROUP BY nr.seq_id, nr.experimental_strategy
),
case_part AS (
    SELECT MAX(se.part) AS part, nr.case_id, nr.experimental_strategy
    FROM new_row nr
    LEFT JOIN {{ mapping.starrocks_staging_sequencing_experiment }} se
    ON se.case_id = nr.case_id AND se.experimental_strategy = nr.experimental_strategy
    GROUP BY nr.case_id, nr.experimental_strategy
),
family_part AS (
    SELECT MAX(se.part) AS part, nr.family_id, nr.experimental_strategy
    FROM new_row nr
    LEFT JOIN {{ mapping.starrocks_staging_sequencing_experiment }} se
    ON se.family_id = nr.family_id AND se.experimental_strategy = nr.experimental_strategy
    GROUP BY nr.family_id, nr.experimental_strategy
),
max_part AS (
    SELECT MAX(se.part) AS part, nr.experimental_strategy
    FROM new_row nr
    LEFT JOIN {{ mapping.starrocks_staging_sequencing_experiment }} se
    ON se.experimental_strategy = nr.experimental_strategy
    GROUP BY nr.experimental_strategy
),
part_count AS (
    SELECT part, experimental_strategy, COUNT(1) AS max_count
    FROM {{ mapping.starrocks_staging_sequencing_experiment }}
    GROUP BY part, experimental_strategy
),
live_task AS (
    SELECT id FROM {{ mapping.clinical_task }}
)
SELECT
    'new' AS state,
    FALSE AS deleted,
    nr.case_id,
    nr.seq_id,
    nr.task_id,
    nr.task_type,
    nr.analysis_type,
    nr.aliquot,
    nr.patient_id,
    nr.experimental_strategy,
    nr.tenant_code,
    nr.request_priority,
    nr.vcf_filepath,
    nr.cnv_vcf_filepath,
    nr.exomiser_filepath,
    nr.sex,
    nr.family_id,
    nr.family_role,
    nr.affected_status,
    nr.histology_type,
    nr.created_at,
    nr.updated_at,
    NULL AS part,
    NULL AS ingested_at,
    pp.part AS patient_part,
    sp.part AS seq_part,
    cp.part AS case_part,
    fp.part AS family_part,
    mp.part AS max_part,
    pc.max_count
FROM new_row nr
LEFT JOIN patient_part pp ON pp.patient_id = nr.patient_id AND pp.experimental_strategy = nr.experimental_strategy
LEFT JOIN seq_part sp ON sp.seq_id = nr.seq_id AND sp.experimental_strategy = nr.experimental_strategy
LEFT JOIN case_part cp ON cp.case_id = nr.case_id AND cp.experimental_strategy = nr.experimental_strategy
LEFT JOIN family_part fp ON fp.family_id = nr.family_id AND fp.experimental_strategy = nr.experimental_strategy
LEFT JOIN max_part mp ON mp.experimental_strategy = nr.experimental_strategy
LEFT JOIN part_count pc ON pc.part = mp.part AND pc.experimental_strategy = nr.experimental_strategy

UNION ALL

SELECT
    'staged' AS state,
    se.deleted OR lt.id IS NULL AS deleted,
    se.case_id,
    se.seq_id,
    se.task_id,
    se.task_type,
    se.analysis_type,
    se.aliquot,
    se.patient_id,
    se.experimental_strategy,
    se.tenant_code,
    se.request_priority,
    se.vcf_filepath,
    se.cnv_vcf_filepath,
    se.exomiser_filepath,
    se.sex,
    se.family_id,
    se.family_role,
    se.affected_status,
    se.histology_type,
    se.created_at,
    se.updated_at,
    se.part,
    se.ingested_at,
    NULL AS patient_part,
    NULL AS seq_part,
    NULL AS case_part,
    NULL AS family_part,
    NULL AS max_part,
    NULL AS max_count
FROM {{ mapping.starrocks_staging_sequencing_experiment }} se
LEFT JOIN live_task lt ON lt.id = se.task_id
WHERE se.updated_at >= COALESCE(se.ingested_at, '1970-01-01 00:00:00') OR se.deleted OR lt.id IS NULL
