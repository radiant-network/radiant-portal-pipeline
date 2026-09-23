# Nextflow CNV Post-processing (from Cases)

Finds the germline cases whose current alignments published a germline CNV VCF that has
never been post-processed, runs the Ferlab
[cnv-post-processing](https://github.com/Ferlab-Ste-Justine/cnv-post-processing) pipeline
over them, and registers the result back onto each case as a
`radiant_germline_cnv_annotation` task and an `exomiser_cnv` task.

`radiant-nextflow-cnv-postprocessing` runs the pipeline and nothing else. This DAG closes
everything around it, exactly as `radiant-nextflow-postprocessing-cases` does for SNV
annotation.

```
discover_scope -> select_cases -> fetch_phenotypes -> resolve_cases -> generate_inputs
                                                                              |
                                                                        run_pipeline
                                                                              |
          register_tasks (one per tenant) <- collect_outputs <----------------+
```

It runs **daily and takes no input**. The parameters exist for targeted reruns and for
configuration, not for normal operation.

## Parameters

| Param | Default | Meaning |
|---|---|---|
| `task_ids` | empty | `alignment_germline_variant_calling` task ids, for a targeted rerun. Empty means "find everything". |
| `tenants` | `$NEXTFLOW_CNV_TENANTS`, else `$NEXTFLOW_POSTPROCESSING_TENANTS` | Tenants the portal has granted this service account `ingest_data` on. Empty means no filtering. |
| `dry_run` | **false** | Passed to the batch PATCH. True validates and writes nothing. |

**`dry_run` on a scheduled run is a trap.** Nothing gets registered, so every case stays
eligible and tomorrow's run does the same work again. Manual runs only.

## What counts as "needs CNV post-processing"

A `(case, sequencing experiment)` pair is eligible when the pair's *current* experiment has
an alignment publishing a `gcnv` / `vcf` document, and no `radiant_germline_cnv_annotation`
task is scoped to that same pair. The rule lives in
`sql/clinical/pending_cnv_annotation_select.sql`, and its two selections are the annotation
query's: **one experiment per (case, member)**, the newest `completed` one, and **one
alignment per experiment**, the newest. Keep the three `pending_*` templates' `current_*`
CTEs identical when touching any of them.

The trigger document is selected on `data_type_code = 'gcnv' AND format_code = 'vcf'`,
never on filename. A case where no member has one (joint-called upstream) is not a
candidate and is not reported.

## What the pipeline is given

One row per member, every row of a family carrying the same `familyPed` and `familyPheno`,
so **every case takes the pipeline's family route** — singletons included. That is what keeps
every output named after `CA<case id>` alone. The samplesheet's `pheno` column (the solo
route) is always empty.

| Column | Value |
|---|---|
| `familyId` | `CA<case id>` |
| `sample` | the sample id, as in the VCF header |
| `sequencingType` | `WGS` or `WES` from the members' strategy (`wxs` → `WES`) |
| `caller` | `DRAGEN` |
| `vcf` | the alignment's `gcnv`/`vcf` document, as a pod path |
| `cram` | the alignment's `alignment`/`cram` document — **only when every member of the family has one whose `crai` is registered at `<cram>.crai`**, empty otherwise |
| `familyPheno`, `familyPed` | written by this DAG, same content as for SNV annotation |

The CRAM rule is all-or-nothing per family on purpose. The pipeline refines a family's
genotypes only when every sample has an alignment, and skips the step with a warning
otherwise — but a CRAM whose index is not at `<cram>.crai` makes mosdepth fail rather than
skip. Passing CRAMs only when the whole set is usable turns both cases into a clean skip.

## Nothing fails the run -- it gets excluded

A candidate that cannot be run is dropped with a reason and the rest continues.
`select_cases` logs them; grep for `excluded`.

| Reason | Meaning | Transient? |
|---|---|---|
| `pending_sequencing` | a member has no `completed` sequencing experiment yet | yes |
| `pending_alignment` | a member's current experiment has no alignment task yet | yes |
| `no_gcnv` | the current alignment published no germline CNV VCF | no |
| `ambiguous_gcnv` | more than one `gcnv`/`vcf` on one alignment task -- a mistyped document | no, fix the data |
| `unsupported_caller` | the alignment's `pipeline_name` is not DRAGEN; the pipeline only implements DRAGEN CNV conventions | no |
| `unknown_caller` | the alignment task has no `pipeline_name`, so `caller=DRAGEN` cannot be asserted | no, record the caller on the task |
| `proband_count` | two different patients are marked proband on the case | no, fix the data |
| `unsupported_strategy` | a strategy outside `{wgs, wxs, wes}`, or members spanning several | no |
| `no_project_code` | `cases.project_id` did not resolve; the batch PATCH needs it | no |
| `tenant_not_granted` | the tenant is not in `tenants` | no, grant it |

If every candidate is excluded the run **skips**. A skipped run is not a failed one.

**A targeted rerun behaves differently on purpose.** With `task_ids` set, an unresolvable
case fails the run instead of being excluded.

## What gets registered

Two tasks per case, through `PATCH /{tenant}/cases/batch`:

| Task | Aliquots | Inputs | Outputs |
|---|---|---|---|
| `radiant_germline_cnv_annotation` | every member | each member's `gcnv` VCF, plus CRAM and index when the run used them | `slivar/CA<id>.cnv.slivar.vcf.gz` (`gcnv`/`vcf`) |
| `exomiser_cnv` | the proband | the slivar VCF above, resolved in-batch | `exomiser/CA<id>.exomiser.{variants.tsv,html,json}` (`exomiser`/`tsv`,`html`,`json`) |

No index on the slivar VCF: the pinned pipeline revision publishes `*.vcf.gz` only. Expecting a
`.tbi` would make every run fail in `collect_outputs`. It is added together with the pin bump to
a revision that indexes it.

`exomiser_cnv` rather than `exomiser`: the variant ETL ingests every `variants.tsv`
published by an `exomiser` task into the SNV Exomiser table, and a CNV report there would
be wrong data. A distinct type keeps it out.

These documents are **invisible to the variant ETL** for now: the
`staging_external_sequencing_experiment` view admits a `gcnv`/`vcf` document only on an
`alignment_germline_variant_calling` task. Ingesting the post-processed CNV VCF into
`germline_cnv_occurrence` is a separate design.

`collect_outputs` requires the complete set for every case in a run; a partial run
registers nothing. PATCH **appends**: a deliberate re-run adds a second pair of tasks
alongside the first.

## Where things are written

| Var | Default | Used by |
|---|---|---|
| `NEXTFLOW_INPUTS_ROOT` | -- | `generate_inputs` writes here (an `s3://` uri); the VCFs and CRAMs must also be under this bucket |
| `NEXTFLOW_OUTPUTS_ROOT` | -- | `collect_outputs` lists here (an `s3://` uri) |
| `NEXTFLOW_INPUTS_MOUNT` | `/workspace/inputs` | the pod path the inputs bucket appears at |
| `NEXTFLOW_OUTPUTS_MOUNT` | `/workspace/outputs` | the pod path the outputs bucket appears at |
| `NEXTFLOW_CNV_TENANTS` | `$NEXTFLOW_POSTPROCESSING_TENANTS` | comma-separated default for the `tenants` param |

```
{NEXTFLOW_INPUTS_ROOT}/cnv-runs/{run_tag}/samplesheet.csv
{NEXTFLOW_INPUTS_ROOT}/cnv-runs/{run_tag}/pedigrees/CA<id>.ped
{NEXTFLOW_INPUTS_ROOT}/cnv-runs/{run_tag}/phenotypes/CA<id>.yml
{NEXTFLOW_OUTPUTS_ROOT}/cnv/{run_tag}/slivar/CA<id>.cnv.slivar.vcf.gz
{NEXTFLOW_OUTPUTS_ROOT}/cnv/{run_tag}/exomiser/CA<id>.exomiser.*
```

The `cnv-runs/` and `cnv/` subdirectories keep these apart from post-processing
(`postprocessing-runs/`, `postprocessing/`) and QC (`qc-runs/`, `qc/`) under the same roots.

The child run id is pinned to `{run_tag}`, so a retry of this DAG re-enters the same
launcher run and its Nextflow launch directory, and `-resume` skips what already completed.
The launcher prefixes its own `cnv-` to it.

## Portal connection

Registration uses the Airflow Connection `radiant_api_conn`: `host` = the API base url,
`login` / `password` = the OIDC client credentials, `extra` =
`{"token_url": "...", "scope": "..."}`. Same connection as the other two cases DAGs.
