# RAD-287 Shared views for the pipeline cases, current experiments, current alignments and case members

Story [RAD-286](https://ferlab-crsj.atlassian.net/browse/RAD-286), Task 1 of 6. Analysis: <https://claude.ai/artifact/AEBifGKjC9oock66MxHAh3> (step 01).

**Summary:** move the rules that pick a case's *current* experiment and alignment, and the case filters they rely on, out of the six selection queries and into four StarRocks views. The queries keep their own trigger documents, anti-joins and exclusion reasons. No behavior change: every query returns the same rows before and after.

## 1. Why

- The supersession policy (SJRA-1698 §5) is copied in four files: "one `completed` experiment per (case, member), newest wins" and "one alignment task per experiment, newest wins". The files ask by comment to keep the copies identical, and nothing checks it.
- The case filter (`status_code NOT IN pipeline_ignored_case_statuses`, by case type) is copied in all six, and in a seventh query: `clinical/case_status_select.sql`, read by the case scheduler to move case statuses.
- To debug "why is this case not picked up?", one has to run a 150 to 200 line query by hand. With views, `SELECT * FROM current_experiment WHERE case_id = …` answers part of it.
- RAD-288 (decision log) and RAD-290 (decisions in Python) build on these views. RAD-289 (rule table) rewrites the staging view on top of them.

## 2. What exists today

| Query | Cases | Current experiment | Current alignment | Members |
|---|---|---|---|---|
| `clinical/pending_annotation_select.sql` | germline | per (case, patient) | `alignment_germline_variant_calling` | `family` |
| `clinical/pending_cnv_annotation_select.sql` | germline | per (case, patient) | `alignment_germline_variant_calling` | `family` |
| `clinical/pending_quality_control_select.sql` | germline | per (case, patient) | `alignment_germline_variant_calling` | `family` |
| `clinical/pending_quality_control_somatic_select.sql` | somatic | proband, `tumoral` only | `alignment_somatic_variant_calling` | proband only |
| `clinical/pending_somatic_annotation_select.sql` | somatic | none (unit = calling task) | none | none |
| `radiant/init/staging_external_sequencing_experiment_create_table.sql` | all | none (every task) | none | `sample → patient → family` |

Notes:

- The germline CNV and QC files say they are "deliberately *not* a shared template" because "the unit tests render each file standalone". This spec revisits that choice: the views keep each file standalone. What they share is a database object, not a Jinja include.
- The portal writes one `family` row for every patient of a case, proband included (`persistFamily`, `backend/cmd/worker/case_validation.go`). `family.id` is that row's id, so there is one per (case, member), not one per family.

## 3. Options

- **A. StarRocks views (chosen).** Created by `radiant-init-starrocks-base-tables` in the base database, over the `radiant_jdbc` catalog, like `staging_external_sequencing_experiment` already is.
  - Pros: one definition; can be queried by hand; the pattern is already in production; each query file stays standalone for the unit tests.
  - Cons: a definition change ships through a migration (drop the view, re-run the init DAG, the RAD-266 pattern); the integration tests must create the views first.
- **B. Jinja macros** (`{% from "clinical/_current.sql" import current_experiment %}`).
  - Pros: no database object, no migration.
  - Cons: cannot be queried by hand; every renderer needs a loader with the right search path (tests use `jinja2.Template(text)` today, the DAGs use `template_searchpath`).
- **C. Keep the copies, add a test** asserting the `current_*` CTEs are textually identical.
  - Pros: smallest change.
  - Cons: fixes neither the debugging problem nor the staging view, and RAD-288 to RAD-290 have nothing to build on.

## 4. The views

All four are created by `CREATE VIEW IF NOT EXISTS` in `radiant/dags/sql/radiant/init/`, with mapping keys added to `radiant/tasks/data/radiant_tables.py` (`starrocks_pipeline_case`, …).

Read together, they answer "what are this case's inputs today?": case → its members → each member's current experiment → that experiment's current alignment. A pending query is that chain, minus what has already been produced downstream.

#### `pipeline_case`

- **Meaning:** the cases the ETL is allowed to see. A draft or revoked case does not exist for the pipelines. The case type (germline or somatic) decides which rules apply to it.
- One row per case the pipelines may look at: `status_code NOT IN ({{ mapping.pipeline_ignored_case_statuses }})`.
- Replaces `germline_case` / `somatic_case` in the five pending queries and the status filter of `sequencing_context` in the staging view.
- `project_code` comes from a `LEFT JOIN project ON project.id = cases.project_id`, on the primary key alone (same as today).

| Column | Source |
|---|---|
| `case_id` | `cases.id` |
| `case_type` | `cases.case_type_code` |
| `status_code`, `tenant_code` | `cases` |
| `submitter_case_id`, `primary_condition`, `project_id`, `proband_id`, `priority_code` | `cases` |
| `project_code` | `project.code` (NULL if unresolved) |

#### `current_experiment`

- **Meaning:** for each person in a case, the sequencing that counts today. A person can be sequenced again (failed QC, new sample); only the latest completed run represents them. A somatic proband has a tumor and a normal sample, so each has its own current run.
- One row per (case, patient), plus histology for somatic cases: the newest `completed` experiment, ranked by `se.created_on DESC, se.id DESC`.
- Ranking: `PARTITION BY case_id, patient_id, CASE WHEN case_type = 'somatic' THEN histology_code END`.
  - Germline: the third key is NULL, so the partition is (case, patient), as today.
  - Somatic: `pending_quality_control_somatic_select.sql` filters `patient_id = proband_id AND histology_code = 'tumoral'` *before* ranking today. Ranking per histology and filtering after gives the same row: the filter removes whole partitions.

| Column | Source |
|---|---|
| `case_id`, `case_type` | `pipeline_case` |
| `patient_id`, `histology_code` | `sample` |
| `seq_id`, `aliquot`, `strategy` | `sequencing_experiment` (`strategy` = `experimental_strategy_code`) |
| `sample_id` | `sample.submitter_sample_id` |

#### `current_alignment`

- **Meaning:** for each sequencing run, the alignment and variant calling result that counts today. A run can be re-aligned (rerun, new pipeline version); the latest result wins. It belongs to the run, not to the case: a run shared by two cases has a single current alignment.
- One row per (experiment, alignment task type): the newest task, ranked by `t.created_on DESC, t.id DESC`.
- Covers `alignment_germline_variant_calling` and `alignment_somatic_variant_calling`. Each consumer filters on `task_type`.
- Joined through `task_context.sequencing_experiment_id`, never `task_context.case_id` (alignment tasks have no case). The comment that explains it moves into the view.

| Column | Source |
|---|---|
| `seq_id` | `task_context.sequencing_experiment_id` |
| `task_type` | `task.task_type_code` |
| `task_id`, `pipeline_name` | `task` |

#### `case_member`

- **Meaning:** who is in the case and in which role (proband, mother, father…), with their affected status and sex. It does not depend on sequencing: a member can be listed before any of their runs is complete.
- One row per `family` row of a `pipeline_case`.
- Replaces the `family` + `patient` joins of the three germline pending queries, and the `family` joins of the staging view and of the somatic QC query. Those two keep reading `patient` directly: a somatic case can have no `family` row for its proband, and its rows still need the sex.

| Column | Source |
|---|---|
| `case_id` | `family.case_id` |
| `patient_id` | `family.family_member_id` |
| `family_row_id` | `family.id` (exposed as `family_id` by the staging view, as today) |
| `role`, `affected_status` | `family.relationship_to_proband_code`, `family.affected_status_code` |
| `sex`, `submitter_patient_id` | `patient` |

## 5. Rewriting the consumers

**Germline pending queries** (`pending_annotation`, `pending_cnv_annotation`, `pending_quality_control`)
- `germline_case`, `current_experiment` and `current_alignment` CTEs removed. They read the views with `case_type = 'germline'` and `task_type = 'alignment_germline_variant_calling'`.
- The member join reads `case_member` instead of `family` + `patient`.
- Unchanged: trigger documents (gVCF, `gcnv` VCF, CRAM, DRAGEN tarball), anti-joins on the downstream task, `exclusion_reason`, `task_ids` and `excluded_tenants` params, row order.
- The "keep the `current_*` CTEs identical" comments are removed.

**`pending_quality_control_somatic_select.sql`**
- `somatic_case`, `current_experiment` and `current_alignment` CTEs removed. A `current_tumor` CTE reads `current_experiment` filtered on `patient_id = proband_id AND histology_code = 'tumoral'`, and `current_alignment` is filtered on `alignment_somatic_variant_calling`.
- The proband's `family` row, when there is one, is a `LEFT JOIN case_member` (it only gives the affected status).

**`pending_somatic_annotation_select.sql`**
- Only the case filter of `registration_case` moves to `pipeline_case`, which also gives the project code (the `project` join goes). No supersession here: the unit of work is the calling task, and every calling task is annotated (tumor-normal and tumor-only on the same sample are two VCFs).

**`staging_external_sequencing_experiment`**
- `sequencing_context` reads `pipeline_case` instead of `cases` + its status filter.
- The final `family` join and the one of `original_alignment` read `case_member` (on `case_id` and the patient). `patient` stays a direct join. The `COALESCE(…, 'proband')` / `'affected'` defaults stay.
- No `current_experiment`: the staging keeps every task of every experiment. This is by design, the import does not apply the supersession policy.

### Example: `pending_quality_control_select.sql`

Only the parts that change are shown. `alignment_document`, `cram`, `crai`, `dragen_metrics` and `quality_controlled` stay as they are.

**Before** (191 lines): the query computes the case filter, the current experiment and the current alignment itself.

```sql
WITH germline_case AS (
    SELECT c.id AS case_id, c.submitter_case_id, c.tenant_code, c.project_id
    FROM {{ mapping.clinical_case }} c
    WHERE c.case_type_code = 'germline'
      AND c.status_code NOT IN ({{ mapping.pipeline_ignored_case_statuses }})
),
current_experiment AS (
    -- The supersession policy: one `completed` experiment per (case, member), newest wins.
    SELECT case_id, patient_id, seq_id, sample_id, aliquot, strategy
    FROM (
        SELECT chse.case_id, s.patient_id, se.id AS seq_id, s.submitter_sample_id AS sample_id,
               se.aliquot, se.experimental_strategy_code AS strategy,
               ROW_NUMBER() OVER (PARTITION BY chse.case_id, s.patient_id
                                  ORDER BY se.created_on DESC, se.id DESC) AS rn
        FROM {{ mapping.clinical_case_has_sequencing_experiment }} chse
        JOIN germline_case gc                               ON gc.case_id = chse.case_id
        JOIN {{ mapping.clinical_sequencing_experiment }} se ON se.id = chse.sequencing_experiment_id
        JOIN {{ mapping.clinical_sample }} s                 ON s.id = se.sample_id
        WHERE se.status_code = 'completed'
    ) ranked WHERE rn = 1
),
current_alignment AS (
    -- Same policy one level down. Joined through sequencing_experiment_id, never case_id.
    SELECT seq_id, task_id
    FROM (
        SELECT tc.sequencing_experiment_id AS seq_id, t.id AS task_id,
               ROW_NUMBER() OVER (PARTITION BY tc.sequencing_experiment_id
                                  ORDER BY t.created_on DESC, t.id DESC) AS rn
        FROM {{ mapping.clinical_task }} t
        JOIN {{ mapping.clinical_task_context }} tc ON tc.task_id = t.id
        WHERE t.task_type_code = 'alignment_germline_variant_calling'
    ) ranked WHERE rn = 1
),
-- alignment_document, cram, crai, dragen_metrics, quality_controlled: unchanged
candidate AS (
    SELECT DISTINCT ce.case_id
    FROM current_experiment ce
    JOIN cram cr                    ON cr.seq_id = ce.seq_id
    LEFT JOIN quality_controlled qc ON qc.case_id = ce.case_id AND qc.seq_id = ce.seq_id
    WHERE qc.seq_id IS NULL
)
SELECT gc.case_id, gc.submitter_case_id, gc.tenant_code, 'germline' AS case_type,
       pr.code AS project_code,
       f.relationship_to_proband_code AS role, f.affected_status_code AS affected_status,
       p.id AS patient_id, p.sex_code AS sex, p.submitter_patient_id,
       ce.sample_id, ce.seq_id, ...,
       CASE ... WHEN pr.code IS NULL THEN 'no_project_code' ... END AS exclusion_reason
FROM germline_case gc
JOIN candidate cd                           ON cd.case_id = gc.case_id
JOIN {{ mapping.clinical_family }} f        ON f.case_id = gc.case_id
JOIN {{ mapping.clinical_patient }} p       ON p.id = f.family_member_id
LEFT JOIN current_experiment ce             ON ce.case_id = gc.case_id AND ce.patient_id = p.id
LEFT JOIN current_alignment ca              ON ca.seq_id = ce.seq_id
-- ... cram, crai, dragen_metrics
LEFT JOIN {{ mapping.clinical_project }} pr ON pr.id = gc.project_id
ORDER BY gc.case_id, CASE f.relationship_to_proband_code ... END, p.id
```

**After**: the query reads the views and keeps only what is specific to QC.

```sql
WITH current_alignment AS (
    SELECT seq_id, task_id
    FROM {{ mapping.starrocks_current_alignment }}
    WHERE task_type = 'alignment_germline_variant_calling'
),
-- alignment_document, cram, crai, dragen_metrics, quality_controlled: unchanged
candidate AS (
    SELECT DISTINCT ce.case_id
    FROM {{ mapping.starrocks_current_experiment }} ce
    JOIN cram cr                    ON cr.seq_id = ce.seq_id
    LEFT JOIN quality_controlled qc ON qc.case_id = ce.case_id AND qc.seq_id = ce.seq_id
    WHERE ce.case_type = 'germline'
      AND qc.seq_id IS NULL
)
SELECT pc.case_id, pc.submitter_case_id, pc.tenant_code, pc.case_type, pc.project_code,
       cm.role, cm.affected_status, cm.patient_id, cm.sex, cm.submitter_patient_id,
       ce.sample_id, ce.seq_id, ...,
       CASE ... WHEN pc.project_code IS NULL THEN 'no_project_code' ... END AS exclusion_reason
FROM {{ mapping.starrocks_pipeline_case }} pc
JOIN candidate cd                                  ON cd.case_id = pc.case_id
JOIN {{ mapping.starrocks_case_member }} cm        ON cm.case_id = pc.case_id
LEFT JOIN {{ mapping.starrocks_current_experiment }} ce
                                                   ON ce.case_id = pc.case_id AND ce.patient_id = cm.patient_id
LEFT JOIN current_alignment ca                     ON ca.seq_id = ce.seq_id
-- ... cram, crai, dragen_metrics
WHERE pc.case_type = 'germline'
ORDER BY pc.case_id, CASE cm.role ... END, cm.patient_id
```

What changes:
- Removed: the `germline_case` and `current_experiment` CTEs, and the `family`, `patient` and `project` joins (about 45 lines).
- `current_alignment` becomes a 3-line filter on the view. It is kept as a CTE so that `alignment_document` does not change.
- Unchanged: the trigger document, the anti-join on `quality_control_metrics`, the exclusion reasons, the `task_ids` and `excluded_tenants` params, and the row order.
- Debugging: "why does the father of case 42 have no experiment?" is now `SELECT * FROM current_experiment WHERE case_id = 42`.

## 6. Behavior

No change intended. Each of the six queries must return the same rows before and after, checked as in §8.

`case_status_select.sql` reads `pipeline_case` too, so the scheduler moves the statuses of exactly the cases the pending queries look at. The status filter now exists in one place.

Two values are now fixed when the views are created, not on each query run:

- `pipeline_ignored_case_statuses`. The list is a constant in `radiant_tables.py`, so changing it already needs a deploy; it will also need `pipeline_case` re-created, which the migration of that change must say. A comment next to the constant says so.
- The clinical catalog and database (`RADIANT_CLINICAL_CATALOG`, `RADIANT_CLINICAL_DATABASE`). The pending queries used to resolve them on each run; nothing overrides them per DAG run today (they come from the environment), and the staging view already worked this way.

Performance: the views are inlined, so a query no longer shares one CTE between its uses, and the `case_type` filter is applied above the window of `current_experiment` rather than pushed into the clinical scan. The clinical tables are small; the `EXPLAIN` of §8 checks it on QA.

## 7. Deployment

- `_BASE_TABLES` in `radiant/dags/init_starrocks_tables.py`: the four views go before `staging_external_sequencing_experiment`, in dependency order (`pipeline_case`, `current_experiment`, `current_alignment`, `case_member`). The init DAG chains its tasks in list order.
- Migration `radiant/dags/sql/radiant/migrations/RAD-287_shared_current_views.sql`, same shape as RAD-266:
  1. `DROP VIEW IF EXISTS staging_sequencing_experiment_delta;` then `DROP VIEW IF EXISTS staging_external_sequencing_experiment;` (the delta reads the external view).
  2. Run **Radiant - Init StarRocks Base Tables**: creates the four views, then re-creates the two staging views.
  3. When: after the ETL deploy, with no `radiant-case-scheduler`, `radiant-import` or `import_part` running.
- Rollback: drop the two staging views and the four new ones, deploy the previous ETL version, re-run the init DAG.

## 8. How this gets verified

**Unit**
- The SQL render tests (`tests/unit/dags/test_clinical_*_sql_render.py`, `test_import_pending_case_sql_render.py`) keep their assertions on params and exclusion reasons. Assertions on the removed CTE names are updated to the view names.

**Integration**
- A fixture creates the four views from their init files before the pending queries run, as `test_sequencing_experiment_select_delta.py` does for the staging views.
- The existing scenarios pass unchanged: trio, shared experiment (`SHARED_CASE_A/B`), superseded experiment, re-aligned experiment, pending case, somatic, already annotated.
- New scenario: a somatic case whose proband has two `completed` tumor experiments and one normal. `pending_quality_control_somatic_select` returns the newer tumor, and `current_experiment` keeps a separate normal row.
- New test: each view on its own (one row per partition key, ranking order, ignored statuses left out).

**QA, before merge**
- For each of the six queries, run the `main` version and the branch version on the QA data. Both `EXCEPT` directions must be empty.
- `EXPLAIN` one pending query before and after, to check the scans on `radiant_jdbc` keep their pushed-down predicates through the views.

## 9. Out of scope

- The common decision schema and its log: RAD-288.
- The task output rules of the staging view: RAD-289.
- Moving the exclusion reasons to Python: RAD-290.

## 10. Open questions

Decided: the views have no prefix (`pipeline_case`, `current_experiment`, `current_alignment`, `case_member`); a future `clinical_v1` interface can rename them.

1. **`family_id` in staging is `family.id`**, one per (case, member), not one per family. `family_part` in `staging_sequencing_experiment_delta` therefore groups nothing more than the case does. To raise in RAD-292 (partition assignment).

## 11. Delivery

One PR in `radiant-portal-pipeline`, branch `feat/rad-287-current-views`: the four views, the six rewritten queries, the mapping keys, the init order, the migration, the tests, and the DAG doc that mentions the copied CTEs (`radiant/dags/docs/nextflow_cnv_postprocessing_cases.md`).
