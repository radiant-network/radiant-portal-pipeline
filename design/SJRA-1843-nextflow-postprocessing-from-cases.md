# Case-driven Nextflow post-processing — analysis

**Status:** analysis / proposal — no code written yet

## 1. Why

Running the Ferlab post-processing pipeline against real cases is currently a three-part
manual exercise, and only the middle part is automated:

| step | today |
|---|---|
| build samplesheet / PED / phenopackets | ad-hoc local scripts, run by hand from pasted StarRocks dumps |
| run the pipeline | `radiant-nextflow-postprocessing` DAG — automated |
| register the outputs as tasks | ad-hoc local scripts, run by hand with a browser-obtained token |

The two manual ends are the fragile ones. They carry constraints that are invisible until
violated — nf-schema rejects a numeric family id, PED and phenopacket filenames are keyed
on that same id, the batch API requires `input_documents`, and only one of the pipeline's
several VCFs is the one to register. This DAG closes both ends so a run is "give me nine
case ids" rather than an afternoon.

## 2. Shape

One new DAG, `radiant-nextflow-postprocessing-cases`, five tasks:

```
resolve_cases → generate_inputs → [DAG] nextflow-postprocessing → collect_outputs → register_tasks
```

It **triggers** the existing pipeline DAG rather than duplicating it
(`TriggerDagRunOperator`, precedent: `radiant/dags/import_radiant.py:191`), so the driver-pod, resume and
cleanup behaviour stay in one place.

### Params

| param | required | meaning |
|---|---|---|
| `case_ids` | yes | list of `cases.id` in the tenant |
| `tenant` | yes | tenant code |
| `dry_run` | no, default `true` | passed to the batch PATCH; makes the last task validate-only |

Only per-run intent is a param. The two storage roots are environment, alongside the other
`RADIANT_*` settings in `radiant/dags/__init__.py`:

```
NEXTFLOW_INPUTS_ROOT     where generated metadata is written
NEXTFLOW_OUTPUTS_ROOT    where the pipeline publishes
```

and both working paths are derived from the run:

```
input prefix   {NEXTFLOW_INPUTS_ROOT}/{run_tag}
outdir         {NEXTFLOW_OUTPUTS_ROOT}/{run_tag}
```

`run_tag` comes from `run_id`, sanitised as the pipeline DAG already does
(`{{ run_id | replace(':','-') | replace('+','-') }}`) — not from `ts_nodash`, because a
manual Airflow 3 run can have a null `logical_date` and date-derived templates then raise at
render time. Retries reuse the same `run_id`, so paths are stable across attempts and
`-resume` still works.

Deriving rather than parameterising also makes "a fresh prefix per run" structural instead
of a rule someone has to remember, and gives each run its own output location — which §11
requires, since re-runs are meant to sit alongside earlier ones rather than replace them.
`outdir` is still passed to the pipeline DAG explicitly rather than letting it derive its
own default, so the coupling between the two DAGs stays visible.

## 3. `resolve_cases` — case ids to a family model

Two queries against the tenant database, as templated SQL under
`radiant/dags/sql/clinical/`, run through `RadiantStarRocksOperator` with an
`output_processor` (the `radiant/dags/import_radiant.py` pattern).

**members** — one row per (case, family member), filtered to the requested `case_ids` and
`case_type_code = 'germline'`:

| from | fields |
|---|---|
| `cases` | `id`, `submitter_case_id`, `primary_condition` |
| `family` | `relationship_to_proband_code`, `affected_status_code` |
| `patient` | `id`, `sex_code`, `submitter_patient_id` |
| `sample` / `sequencing_experiment` | `submitter_sample_id`, `aliquot`, `experimental_strategy_code` |
| `document` | the member's gVCF URL — see §3.1 |

Ordered proband-first (`proband`, `father`, `mother`, then the rest), which is what the PED
and phenopacket writers assume.

**phenotypes** — HPO terms per (case, patient), from `obs_categorical` where
`observation_code = 'phenotype'` and `coding_system = 'HPO'`, with the label joined from the
shared `radiant.hpo_term` dictionary and `interpretation_code = 'negative'` meaning excluded.

Two details these queries have to get right:

### 3.1 Resolving each member's gVCF

Select the gVCF on the document's own type fields, not on its filename:

```sql
JOIN task_has_document thd ON thd.task_id = t.id AND thd.type = 'output'
JOIN document d            ON d.id = thd.document_id
                          AND d.data_type_code = 'snv'      -- "Germline SNV"; ssnv is somatic
                          AND d.format_code    = 'gvcf'
```

The code is **`snv`**, not `gsnv` — the `data_type` dictionary has no `gsnv`; germline is
`snv` and somatic is `ssnv`, and a gVCF is that data type in the `gvcf` file format.

That filter identifies gVCFs, but on its own it does not say *which member* one belongs to.

An alignment task may be linked to more than one sequencing experiment and publish a gVCF
for each. Joining `task → task_context → sequencing_experiment` then yields a cross
product: every member of that group resolves to every gVCF in it, and nothing in the
relational path distinguishes them. Only the file itself does.

Whatever rule is used to attribute them (see the annex query), **assert exactly one gVCF per
member**. A count of 0 or >1 should fail the task rather than produce a quietly wrong
samplesheet — that assertion is the actual safeguard.

It also catches bad data. An index file recorded with the gVCF's `format_code` instead of
`tbi`, for instance, is picked up by the type filter as though it were data — an error to
correct at the source rather than to encode a permanent workaround for, but one the count
will catch either way.

**Only `germline` cases.** The pipeline's `step: genotype` entry point assumes germline
joint calling. `resolve_cases` should reject a somatic case id rather than silently produce
a samplesheet the pipeline mis-handles.

## 4. `generate_inputs` — samplesheet, PED, phenopackets

A writer under `radiant/tasks/nextflow/`, taking the `resolve_cases` XCom and emitting:

```
{input_prefix}/samplesheet.csv
{input_prefix}/pedigrees/{familyId}.ped
{input_prefix}/phenotypes/{familyId}.yml
```

and the samplesheet references the other two by **pod path** (`/workspace/inputs/…`), not
S3 URI — FSx auto-imports the `inputs` prefix, so writing to S3 is enough and the task needs
no PVC mount. That keeps this a plain Airflow task rather than a `KubernetesPodOperator`.

Columns are fixed by the pipeline schema:
`familyId,sample,sequencingType,gvcf,familyPheno,familyPed`.

Three constraints to encode:

- **`familyId` is `CA` + the case id** — `CA1072`, not `1072` and not `submitter_case_id`.
  The prefix is not cosmetic: nf-schema coerces a bare numeric id to a number and then fails
  `type: string` with the misleading message *"familyId must be provided and cannot contain
  spaces"*. Quoting does not help; a non-numeric prefix does. Deriving it from `cases.id`
  rather than `submitter_case_id` also makes it stable and collision-free — a submitter id
  is free text, can change, and carries no uniqueness guarantee.
- **`familyId` keys the filenames too.** It is not only a column — the PED and phenopacket
  are named after it, and so are the pipeline's outputs, which is how §6 finds them again.
- **`sequencingType`** is `WGS`/`WES` uppercase, from `experimental_strategy_code`.

The task should write to a **fresh** prefix per run, or delete the prefix first, so a
regenerated set never sits beside a stale one.

## 5. Triggering the pipeline

```python
TriggerDagRunOperator(
    trigger_dag_id=f"{NAMESPACE}-nextflow-postprocessing",
    conf={"input": f"{input_prefix}/samplesheet.csv", "outdir": outdir},  # both run-derived, §2
    wait_for_completion=True,
    reset_dag_run=True,
    deferrable=True,
)
```

`max_active_runs=1` on the child DAG means two parents queue rather than collide — correct,
since they would share the FSx workspace, but it does mean a second run waits for the first.
Worth stating in the DAG doc so a queued run is not read as a hang.

The child's `cleanup_work` step only removes scratch on success, so `-resume` still works
across retries of a failed run.

## 6. `collect_outputs` — find what the run produced

List `{outdir}/slivar/` and `{outdir}/exomiser/` and match per family on the `CA<case id>`
key the samplesheet was written with:

| file | goes to |
|---|---|
| `variants.{familyId}.snv.vep.slivar.vcf.gz` (+ `.tbi`) | `radiant_germline_annotation` outputs |
| `{familyId}.exomiser.variants.tsv` | `exomiser` outputs |
| `{familyId}.exomiser.html`, `.json` | `exomiser` outputs |

Because `familyId` is derived from `cases.id`, going from a filename back to a case is
arithmetic rather than a lookup. Note the two keys are not the same: `familyId` matches
files, while the batch PATCH in §7 resolves cases by `(project_code, submitter_case_id)` —
`resolve_cases` returns both, so each is used where it belongs.

Sizes come from the S3 listing rather than being trusted from anywhere — the batch API
compares `size` against any existing document with the same URL and raises `DOCUMENT-006`
on a mismatch, so a stale size fails the batch rather than corrupting it.

**Fail if a family is missing a file.** A partially-successful Nextflow run must not produce
a partially-registered case set, and a task failure is not a reliable signal on its own —
outputs can be complete while the task is marked failed, and vice versa. Only a per-family
assertion over the listing distinguishes the two.

## 7. `register_tasks` — the batch PATCH

Reuse `radiant-portal/cli/python` (`CasesApi.patch_case_batch`), packaged into the task
image rather than `sys.path`-injected as the throwaway script does.

PATCH, not POST: the cases already exist, are looked up by `(project_code, submitter_case_id)`,
and **array fields append** — so the existing `alignment_germline_variant_calling` tasks
survive. It is also idempotent enough to re-run.

Per case, two tasks:

| | `radiant_germline_annotation` | `exomiser` |
|---|---|---|
| aliquots | every family member | proband only |
| input_documents | each member's gVCF URL | the slivar VCF (resolved in-batch) |
| output_documents | slivar `vcf` + `tbi` | `tsv` + `html` + `json` |
| pipeline | `Post-processing-Pipeline` / `3.0.0` | `Exomiser` / `14.0.0` |

Four rules the backend enforces, any of which fails the whole batch:

- **`input_documents` is mandatory** for both types — they are in
  `RequiresInputDocumentsTaskTypes`. Omitting it is `TASK-003`.
- **Inputs must resolve** to a document already in the tenant, or to an output of another
  task *in the same batch* (`TASK-005`). This is why exomiser's input is the slivar VCF and
  not the `ensemblvep` VCF the pipeline actually consumes — the latter is registered
  nowhere. See §9.
- **`exomiser` is single-aliquot** (`SingleAliquotTaskTypes`); more than one is `TASK-007`.
- **The ETL reads only two shapes.** `staging_external_sequencing_experiment` derives
  `vcf_filepath` from `format_code='vcf'` on an *output* doc of a `radiant_*_annotation`
  task, and `exomiser_filepath` from `url LIKE '%variants.tsv'` on an exomiser task. The VCF
  must therefore be declared `vcf`, not `gvcf`; the html/json ride along for the portal and
  are invisible to the ETL.

Run with `dry_run=true` first — the batch report names every failure with its code and path,
which is far better triage than an HTTP status.

## 8. Authentication

The DAG uses the OAuth **client_credentials** grant — a client id and secret in env vars,
no browser and no device approval.

That needs a dedicated Keycloak service-account client, provisioned with the permissions to
ingest into each tenant the DAG writes to. A valid token is not sufficient on its own: the
portal authorises against its own permission store (tenant access + the `ingest_data`
action), not against realm roles, so an unprovisioned client gets a flat 403 before any
validation runs. The existing `hybrid_system` client demonstrates exactly that today.

Config, matching the `RADIANT_TASK_OPERATOR_*` convention in `radiant/dags/__init__.py`:

```
RADIANT_API_URL          https://api.dev.qlin.aws.sante.quebec
RADIANT_OIDC_TOKEN_URL   https://auth.dev.qlin.aws.sante.quebec/realms/qlin/protocol/openid-connect/token
RADIANT_OIDC_CLIENT_ID   from a Secret, not a literal
RADIANT_OIDC_CLIENT_SECRET  ditto — ESO-synced, mounted as env, never logged
```

## 9. Known compromise: exomiser input lineage

The pipeline feeds Exomiser the **VEP-annotated** VCF (`exomiser_start_from_vep = true`;
the driver log says so explicitly), one step before slivar. That file is not published as a
document, so naming it as an input fails `TASK-005`. Recording the slivar VCF instead makes
the graph one step downstream of the truth.

Two ways out, if the lineage matters:

1. Register the `ensemblvep` outputs as documents too — either on the annotation task as an
   extra output, or on a third task type. Costs storage bookkeeping, gains an accurate graph.
2. Accept it, and say so in the DAG doc.

Either is defensible; the point is to choose explicitly rather than let the constraint
decide by default.

## 10. Scope boundary: cases without gVCFs

Not every germline case has per-sample gVCFs. Some were joint-called upstream: their
alignment tasks publish only CRAM and CNV, and the variants arrive through a
`family_variant_calling` task that emits a single joint VCF. `resolve_cases` returns
`gvcf_matches = 0` for every member of such a case and rejects it, which is correct — with
no gVCFs there is nothing for `step: genotype` to do.

These cases are **out of scope** for this DAG. They do have a real need — the same
sequencing data can be re-examined under a different clinical framing, with a different
primary condition and different affected statuses across the family, which changes both the
PED and the phenopacket — but what they need is Exomiser re-run against the existing joint
VCF, not the full pipeline. That is a separate, narrower job and should be designed as one.

## 11. Re-runs sit alongside

Running the same cases again does not supersede the earlier analysis. PATCH appends, so the
case gains a second `radiant_germline_annotation` and `exomiser` task pointing at the new
run's outputs, and both remain queryable.

That is the model the platform already implements, not a compromise. `staging_sequencing_experiment`
keys on `(case_id, seq_id, task_id)`, so each task is its own row with its own
`vcf_filepath`; the portal exposes `GET /{tenant}/cases/{case_id}/{seq_id}/tasks_with_occurrences`
— plural, with `created_on` per task — so a user can pick between them. Cases carrying
several annotation tasks over the same sequencing experiments already exist.

The consequence for this DAG is only that each run needs its own output location, which
§2 gets for free by deriving the path from the run.

## 12. Open questions

- **Which tenants?** Grants in §8 are per tenant; the DAG takes `tenant` as a param, so the
  set has to be known.
- **`project_code`** is required for the PATCH lookup and should come from `resolve_cases`.
  Note that `cases.project_id` does not necessarily match a row in `project` — worth
  understanding before relying on that join.
- **Exomiser version** `14.0.0` is inherited from existing seed data, not read
  from the container (`ferlabcrsj/exomiser:2.4.1`, data `2402`). Someone should confirm the
  real software version before it becomes load-bearing metadata.

## Annex — `resolve_cases` SQL

Both queries are tenant-scoped through the `mapping` dict the StarRocks operators inject
(see `radiant/tasks/data/radiant_tables.py`), and take the requested ids as a bound
parameter, following `radiant/dags/sql/radiant/sequencing_experiment_delete.sql`.

They are written below as plain SQL, runnable as-is in a client. **In the DAG they are not.**
`RadiantStarRocksOperator` ends at `cursor.execute(sql, parameters)`, so once a statement
carries a bound parameter every literal `%` has to be doubled —
`concat(s.submitter_sample_id, '%%')`. The one existing template that uses `LIKE '%…'`
(`staging_external_sequencing_experiment_create_table.sql`) runs without parameters and so
never hit this.

### A. members

One row per (case, family member). `gvcf_matches` is the assertion handle from §3.1: the
task should fail unless every row is exactly `1`.

```sql
WITH gvcf_doc AS (
    SELECT tc.sequencing_experiment_id                              AS seq_id,
           d.url,
           d.name,
           COUNT(*) OVER (PARTITION BY tc.sequencing_experiment_id) AS n_candidates
    FROM {{ mapping.clinical_task }} t
    JOIN {{ mapping.clinical_task_context }}      tc  ON tc.task_id = t.id
    JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = t.id
                                                     AND thd.type = 'output'
    JOIN {{ mapping.clinical_document }}          d   ON d.id = thd.document_id
                                                     AND d.data_type_code = 'snv'
                                                     AND d.format_code    = 'gvcf'
    WHERE t.task_type_code = 'alignment_germline_variant_calling'
)
SELECT c.id                           AS case_id,
       c.submitter_case_id,
       c.primary_condition,
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
-- One candidate: take it. Several (a task spanning siblings): fall back to the sample id.
LEFT JOIN gvcf_doc g ON g.seq_id = se.id
                    AND (g.n_candidates = 1
                         OR g.name LIKE concat(s.submitter_sample_id, '%'))
WHERE c.id IN %(case_ids)s
  AND c.case_type_code = 'germline'
GROUP BY c.id, c.submitter_case_id, c.primary_condition,
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
         p.id;
```

Run against the existing tenants this returns no ambiguity — every member with a registered
gVCF resolves to exactly one, including the members whose gVCF comes from a task shared with
their siblings. The rows that come back `gvcf_matches = 0` are members with no gVCF
registered at all, which the DAG must reject rather than paper over.

### B. phenotypes

HPO terms per (case, patient). `interpretation_code = 'negative'` means the term was
explicitly excluded, so the phenopacket writer must not emit it as an observed feature.

```sql
SELECT oc.case_id,
       oc.patient_id,
       oc.code_value          AS hpo_id,
       h.name                 AS hpo_label,
       oc.onset_code,
       oc.interpretation_code
FROM {{ mapping.clinical_obs_categorical }} oc
LEFT JOIN {{ mapping.hpo_term }} h ON h.id = oc.code_value
WHERE oc.case_id IN %(case_ids)s
  AND oc.observation_code = 'phenotype'
  AND oc.coding_system    = 'HPO'
ORDER BY oc.case_id, oc.patient_id, oc.code_value;
```

`hpo_term` is a shared dictionary in the base database, not tenant-scoped — hence the
`LEFT JOIN`, so an unknown code yields a null label rather than dropping the row.
