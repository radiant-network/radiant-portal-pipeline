# Nextflow Post-processing (from Cases)

Finds the germline cases that have been aligned but never annotated, runs the Ferlab
[Post-processing-Pipeline](https://github.com/Ferlab-Ste-Justine/Post-processing-Pipeline)
over them, and registers what it produces back onto those cases.

`radiant-nextflow-postprocessing` runs the pipeline and nothing else. This DAG closes
everything around it — deciding what needs running, building the pipeline's inputs from the
clinical model, and turning its outputs into portal tasks.

```
discover_scope -> select_cases -> fetch_phenotypes -> resolve_cases -> generate_inputs
                                                                             |
                                                                       run_pipeline
                                                                             |
         register_tasks (one per tenant) <- collect_outputs <----------------+
```

It runs **daily and takes no input**. The parameters below exist for targeted reruns and for
configuration, not for normal operation.

## Parameters

| Param | Default | Meaning |
|---|---|---|
| `task_ids` | empty | `alignment_germline_variant_calling` task ids, for a targeted rerun. Empty means "find everything". |
| `tenants` | `$NEXTFLOW_POSTPROCESSING_TENANTS` | Tenants the portal has granted this service account `ingest_data` on. Empty means no filtering. |
| `dry_run` | **false** | Passed to the batch PATCH. True validates and writes nothing. |

Nothing else is a param. Paths are derived from the run (below), everything run-invariant —
reference genome, VEP cache, Exomiser data, `tools`, `step` — lives in the `nextflow-params`
ConfigMap the pipeline DAG reads, and the **tenant is read off the cases**.

That last one matters. The clinical tables are a single shared schema behind the
`radiant_jdbc` catalog, and `cases.id` is a plain single-column primary key over all of it
(`case_pkey`), with `tenant_code` only an attribute — so a case id already names its tenant.
A run may span several tenants; registration groups by tenant and sends one batch each.

**`dry_run` on a scheduled run is a trap.** A dry run registers nothing, so every case stays
eligible and tomorrow's run does the same work again, for ever. Use it for a manual run
only.

## What counts as "needs annotating"

A `(case, sequencing experiment)` pair is eligible when the pair's *current* experiment has
an alignment publishing a gVCF, and no `radiant_germline_annotation` is scoped to that same
pair. The whole rule lives in `sql/clinical/pending_annotation_select.sql`.

Two selections make it work, and both matter more than they look:

- **one sequencing experiment per (case, member)** — the newest `completed` one;
- **one alignment task per experiment** — the newest.

The same selection decides eligibility *and* builds the family. If it did not, a superseded
experiment would still have an alignment and no annotation, so the case would be
re-discovered and the pipeline re-run every night, for ever.

Consequences worth knowing at the console:

| You see | Because |
|---|---|
| a case re-annotated after a member was re-sequenced | the newer experiment became current; the earlier annotation is untouched and stays queryable |
| a case's parents re-annotated when only the proband changed | a joint call over a different proband sample is a different result for every member. A family is one unit |
| two families in one run naming the same gVCF | one experiment linked to two cases. Each is its own family and gets its own annotation task |
| a case that never appears | it has no gVCFs at all (joint-called upstream, out of scope), it is `revoke`d, it is somatic, or it is already annotated |

## Nothing fails the run any more — it gets excluded

A candidate that cannot be run is dropped with a reason and the rest of the run continues.
One unfixable case must not block every other case, every night. `select_cases` logs each
one; grep its task log for `excluded`.

| Reason | Meaning | Transient? |
|---|---|---|
| `pending_sequencing` | a member has no `completed` sequencing experiment yet | yes |
| `pending_alignment` | a member's current experiment has no alignment task yet | yes |
| `no_gvcf` | the current alignment published no gVCF — joint-called upstream, out of scope | no |
| `ambiguous_gvcf` | the current alignment published more than one gVCF. One task cannot legitimately do that, so a document is mistyped at the source — typically an index recorded with `format_code = 'gvcf'` | no, fix the data |
| `proband_count` | two different patients are marked proband on the case | no, fix the data |
| `unsupported_strategy` | a strategy outside `{wgs, wxs, wes}`, or several within one case | no |
| `no_project_code` | `cases.project_id` did not resolve; the batch PATCH needs it to look the case up | no |
| `tenant_not_granted` | the tenant is not in `tenants`. Caught here rather than as a 403 after hours of pipeline | no, grant it |

The two `pending_*` reasons are normal traffic, not errors. A member whose newest experiment
has no alignment yet makes the case **wait** rather than run against the superseded one and
produce an annotation that is obsolete the moment it lands.

If every candidate is excluded, `select_cases` **skips** and the run ends there. A skipped
run is not a failed one.

**A targeted rerun behaves differently on purpose.** With `task_ids` set, an unresolvable
case raises instead of being excluded — you asked for that specific work, so silence would
be the wrong answer. A named task that yields no candidate case is also reported: it does
not exist, is not an alignment task, or every case it belongs to is already annotated.

## Where things are written

Both roots are environment, not params:

| Var | Default | Used by |
|---|---|---|
| `NEXTFLOW_INPUTS_ROOT` | — | `generate_inputs` writes here (an `s3://` uri) |
| `NEXTFLOW_OUTPUTS_ROOT` | — | `collect_outputs` lists here (an `s3://` uri) |
| `NEXTFLOW_INPUTS_MOUNT` | `/workspace/inputs` | the pod path the inputs bucket appears at |
| `NEXTFLOW_OUTPUTS_MOUNT` | `/workspace/outputs` | the pod path the outputs bucket appears at |
| `NEXTFLOW_POSTPROCESSING_TENANTS` | empty | comma-separated default for the `tenants` param |

Each root gets a subdirectory named after the run:

```
{NEXTFLOW_INPUTS_ROOT}/postprocessing-runs/{run_tag}/samplesheet.csv
{NEXTFLOW_INPUTS_ROOT}/postprocessing-runs/{run_tag}/pedigrees/{familyId}.ped
{NEXTFLOW_INPUTS_ROOT}/postprocessing-runs/{run_tag}/phenotypes/{familyId}.yml
{NEXTFLOW_OUTPUTS_ROOT}/postprocessing/{run_tag}/{slivar,exomiser}/…
```

The `postprocessing-runs/` and `postprocessing/` subdirectories keep these apart from quality
control, which writes `qc-runs/` and `qc/` under the same two roots.

`run_tag` comes from `run_id`, not from a timestamp — an Airflow 3 manual run can have a
null `logical_date`, and date-derived templates raise at render time. It is stable across
retries, so the paths are too.

The tag also drops the `__` after the run type (`scheduled__…` becomes `scheduled-…`). Airflow
3.2 refuses an operator-triggered run whose id starts with `scheduled__`, since that prefix is
reserved for scheduled runs, and reports it as an opaque 500 from the API server.

The Airflow tasks only ever touch S3; the samplesheet and the `outdir` handed to the
pipeline carry **pod paths**. `/workspace` is the FSx-Lustre filesystem: its `inputs`
prefix is auto-imported from the inputs bucket and `outputs` auto-exported to the outputs
bucket, so writing to S3 is enough for the pipeline to see the file, and this DAG needs no
PVC mount.

`generate_inputs` clears the run's input prefix before writing, so a retry never leaves a
regenerated samplesheet sitting beside a stale PED.

## `familyId` is `CA` + the case id

`CA1072`, not `1072` and not `submitter_case_id`.

The prefix is not cosmetic. nf-schema types `familyId` as a string, but its CSV reader
coerces a bare numeric value to a number first, and validation then fails with the
property's single, misleading error message: *"familyId must be provided and cannot contain
spaces"*. Quoting the field does not help; a non-numeric prefix does.

Deriving it from `cases.id` rather than `submitter_case_id` also makes it stable and
collision-free — a submitter id is free text and carries no uniqueness guarantee — and it
is what makes reading a case back out of an output filename arithmetic rather than a lookup.

## What still fails the run

- **A family is missing any of its five output files.** A partially-successful Nextflow run
  must not become a partially-registered case set, and the pipeline task's status is not a
  reliable signal on its own — outputs can be complete while the task is marked failed, and
  the reverse. Only the per-family assertion over the S3 listing tells the two apart.
  `collect_outputs` retries the listing for a few minutes first, because the FSx export is
  a best-effort drain rather than a barrier.
- **The portal accepts a batch but reports no batch id.** Nothing is registered, so the case
  would stay eligible and the pipeline would redo it every night. Failing is the only honest
  outcome.
- **Any tenant's batch fails.** Only that tenant's mapped `register_tasks` instance fails;
  the others keep their registrations.

## Registration

`register_tasks` PATCHes `/{tenant}/cases/batch`, **once per tenant**, as a mapped task.
PATCH, not POST: the cases already exist, are looked up by
`(project_code, submitter_case_id)`, and array fields **append** — so the existing
`alignment_germline_variant_calling` tasks survive.

It is mapped rather than a loop because PATCH appends: one task looping over tenants that
failed halfway and was retried would double-register every tenant that had already
succeeded, producing a second annotation task per case that is indistinguishable from a
legitimate re-run. Mapping gives per-tenant retry. **Clear one mapped instance, not the
whole task.**

Per case, two tasks:

| | `radiant_germline_annotation` | `exomiser` |
|---|---|---|
| aliquots | every family member | proband only |
| input_documents | each member's gVCF | the slivar VCF |
| output_documents | slivar `vcf` + `tbi` | `tsv` + `html` + `json` |
| pipeline | `Post-processing-Pipeline` / `3.0.0` | `Exomiser` / `14.0.0` |

**Known compromise: Exomiser's input lineage.** The pipeline actually feeds Exomiser the
VEP-annotated VCF (`exomiser_start_from_vep = true`), one step before slivar. That file is
published as no document, so naming it as an input fails `TASK-005`. We record the slivar
VCF instead, which puts the recorded lineage one step downstream of the truth. This was
chosen deliberately over registering the `ensemblvep` outputs as documents too; if the
lineage ever matters, that is the fix.

For a first manual run, use `dry_run=true`. The batch report names every failure with its
code and path, which is far better triage than an HTTP status.

### Authentication

Credentials come from the `radiant_api_conn` Airflow Connection:

| field | value |
|---|---|
| `host` | API base url, e.g. `https://api.dev.qlin.aws.sante.quebec` |
| `login` | OIDC client id |
| `password` | OIDC client secret |
| `extra` | `{"token_url": "https://auth…/realms/qlin/protocol/openid-connect/token", "scope": "openid"}` |

The grant is `client_credentials` — no browser, no device approval. **A valid token is not
sufficient.** The portal authorises against its own permission store — tenant access plus
the `ingest_data` action — not against realm roles, so a service-account client that has
not been granted those receives a flat 403 before any validation runs. If `register_tasks`
fails with a 403 and no error codes, that grant is what is missing, not the payload — and
the tenant should go out of `tenants` until it is fixed, so the pipeline stops spending
hours on cases it cannot register.

## Re-runs sit alongside

Running the same cases again does not supersede the earlier analysis. PATCH appends, so the
case gains a *second* `radiant_germline_annotation` and `exomiser` task pointing at the new
run's outputs, and both stay queryable.

That is the platform's own model, not a compromise: `staging_sequencing_experiment` keys on
`(case_id, seq_id, task_id)`, so each task is its own row with its own `vcf_filepath`, and
the portal serves `GET /{tenant}/cases/{case_id}/{seq_id}/tasks_with_occurrences` — plural,
with `created_on` per task — so a user can pick between them.

## A queued run is not a hang

The pipeline DAG is `max_active_runs=1`, because concurrent drivers would share the FSx
workspace. This DAG is too, and that is the entire concurrency story: a run that overruns a
day makes the next one queue rather than start, and because discovery happens at the *start*
of a run, the queued one re-queries after the previous has registered and sees the shorter
list. No case is picked up twice, and no lock or marker column is involved.

On WGS the wait can be hours.

`run_pipeline` pins the child's run id to this run's tag. Without that, retrying this DAG
would spawn a *fresh* child run with a fresh Nextflow launch directory, and `-resume` would
find nothing — turning a retry into a full re-run.

## The first run is a backfill

There is no cap on the number of cases per run. The first scheduled run therefore picks up
every case ever aligned and never annotated, which can be hundreds of families in a single
samplesheet and a single multi-day Nextflow run whose failure loses all of it.

Do the first pass by hand, in tranches, with explicit `task_ids` — then let the schedule
take over on a steady-state delta.

## Out of scope: cases without gVCFs

Not every germline case has per-sample gVCFs. Some were joint-called upstream: their
alignment tasks publish only CRAM and CNV, and the variants arrive through a
`family_variant_calling` task emitting a single joint VCF. These never become candidates —
with no gVCFs there is nothing for `step: genotype` to do — and they are absent from the
exclusion report rather than filling it nightly.

They do have a real need: the same sequencing data re-examined under a different clinical
framing, with a different primary condition and different affected statuses, which changes
both the PED and the phenopacket. But what that needs is Exomiser re-run against the
existing joint VCF, not the full pipeline. It is a separate, narrower job.
