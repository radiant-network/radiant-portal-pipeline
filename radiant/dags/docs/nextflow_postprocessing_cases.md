# Nextflow Post-processing (from Cases)

Runs the Ferlab [Post-processing-Pipeline](https://github.com/Ferlab-Ste-Justine/Post-processing-Pipeline)
against a list of case ids, and registers what it produces back onto those cases.

`radiant-nextflow-postprocessing` runs the pipeline and nothing else. This DAG closes the
two ends around it — building the pipeline's inputs from the clinical model, and turning
its outputs into portal tasks — so a run is "give me nine case ids" rather than an
afternoon of local scripts and pasted query results.

```
fetch_members ----+
                  +-> resolve_cases -> generate_inputs -> run_pipeline
fetch_phenotypes -+                                            |
                                    register_tasks <- collect_outputs
```

## Parameters

| Param | Required | Meaning |
|---|---|---|
| `case_ids` | yes | `cases.id` values. Germline only, and all in one tenant. |
| `dry_run` | no, default **true** | Passed to the batch PATCH. True means validate and write nothing. |

Nothing else is a param. Paths are derived from the run (below), everything run-invariant —
reference genome, VEP cache, Exomiser data, `tools`, `step` — lives in the `nextflow-params`
ConfigMap the pipeline DAG reads, and the **tenant is read off the cases**.

That last one matters. The clinical tables are a single shared schema behind the
`radiant_jdbc` catalog, and `cases.id` is a plain single-column primary key over all of it
(`case_pkey`), with `tenant_code` only an attribute — so a case id already names its tenant.
Asking for it as well would add nothing but a way to name a correct case under the wrong
tenant and get an empty result back. The batch PATCH is addressed to whatever tenant the
cases themselves carry, so it cannot write anywhere the data did not come from.

Because one batch PATCH goes to one tenant, `resolve_cases` rejects a set spanning several
rather than registering part of it. Split the run.

## Where things are written

Both roots are environment, not params:

| Var | Default | Used by |
|---|---|---|
| `NEXTFLOW_INPUTS_ROOT` | — | `generate_inputs` writes here (an `s3://` uri) |
| `NEXTFLOW_OUTPUTS_ROOT` | — | `collect_outputs` lists here (an `s3://` uri) |
| `NEXTFLOW_INPUTS_MOUNT` | `/workspace/inputs` | the pod path the inputs bucket appears at |
| `NEXTFLOW_OUTPUTS_MOUNT` | `/workspace/outputs` | the pod path the outputs bucket appears at |

Each root gets a subdirectory named after the run:

```
{NEXTFLOW_INPUTS_ROOT}/{run_tag}/samplesheet.csv
{NEXTFLOW_INPUTS_ROOT}/{run_tag}/pedigrees/{familyId}.ped
{NEXTFLOW_INPUTS_ROOT}/{run_tag}/phenotypes/{familyId}.yml
{NEXTFLOW_OUTPUTS_ROOT}/{run_tag}/{slivar,exomiser}/…
```

`run_tag` comes from `run_id`, not from a timestamp — an Airflow 3 manual run can have a
null `logical_date`, and date-derived templates raise at render time. It is stable across
retries, so the paths are too.

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

## What fails the run, and why

Each of these fails a task rather than producing something merely plausible. A wrong
samplesheet costs hours of pipeline time before anyone finds out.

- **A requested case id resolves to nothing.** Either the id does not exist, or it is a
  somatic case. The pipeline's `step: genotype` entry point assumes germline joint calling,
  so a somatic id would be silently mis-handled.
- **The requested cases span several tenants.** One batch PATCH is addressed to one tenant.
- **A member does not resolve to exactly one gVCF.** Zero means the case was joint-called
  upstream and has no per-sample gVCFs — out of scope, see below. More than one means
  either a genuinely ambiguous alignment task or a mistyped document at the source (an
  index recorded with `format_code = 'gvcf'`, for instance).
- **A family is missing any of its five output files.** A partially-successful Nextflow run
  must not become a partially-registered case set, and the pipeline task's status is not a
  reliable signal on its own — outputs can be complete while the task is marked failed, and
  the reverse. Only the per-family assertion over the S3 listing tells the two apart.
  `collect_outputs` retries the listing for a few minutes first, because the FSx export is
  a best-effort drain rather than a barrier.

## Registration

`register_tasks` PATCHes `/{tenant}/cases/batch`. PATCH, not POST: the cases already exist,
are looked up by `(project_code, submitter_case_id)`, and array fields **append** — so the
existing `alignment_germline_variant_calling` tasks survive.

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

Run with `dry_run=true` first. The batch report names every failure with its code and path,
which is far better triage than an HTTP status.

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
fails with a 403 and no error codes, that grant is what is missing, not the payload.

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
workspace. Two runs of this DAG therefore queue rather than collide: the second one's
`run_pipeline` task sits deferred until the first finishes. That is correct behaviour, and
on WGS the wait can be hours.

`run_pipeline` pins the child's run id to this run's tag. Without that, retrying this DAG
would spawn a *fresh* child run with a fresh Nextflow launch directory, and `-resume` would
find nothing — turning a retry into a full re-run.

## Out of scope: cases without gVCFs

Not every germline case has per-sample gVCFs. Some were joint-called upstream: their
alignment tasks publish only CRAM and CNV, and the variants arrive through a
`family_variant_calling` task emitting a single joint VCF. `resolve_cases` rejects these,
which is correct — with no gVCFs there is nothing for `step: genotype` to do.

They do have a real need: the same sequencing data re-examined under a different clinical
framing, with a different primary condition and different affected statuses, which changes
both the PED and the phenopacket. But what that needs is Exomiser re-run against the
existing joint VCF, not the full pipeline. It is a separate, narrower job.
