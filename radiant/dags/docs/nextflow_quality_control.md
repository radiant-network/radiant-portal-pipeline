# Nextflow Quality Control

Runs the Ferlab [quality-control-pipeline](https://github.com/Ferlab-Ste-Justine/quality-control-pipeline)
on qlin-eks in its **DRAGEN-metrics mode**: the samples have already been through
DRAGEN, so the MultiQC report is built from DRAGEN's per-sample metric CSVs instead
of being recomputed. Somalier still runs, against whatever BAM/CRAM the samplesheet
lists, for pedigree validation.

One Airflow task launches a Nextflow **driver** pod in the `nextflow` namespace. The
driver spawns one worker pod per pipeline process itself, through Nextflow's
Kubernetes executor — so a single task in the Airflow UI is a fan-out of pods in
Kubernetes. Watch them with:

```sh
kubectl -n nextflow get pods -w
```

## Parameters

| Param | Required | Meaning |
|---|---|---|
| `input` | yes | Absolute path to the samplesheet CSV on the shared workspace. Columns below. |
| `dragen_metrics_dir` | yes | Absolute path to the directory of DRAGEN per-sample metric CSVs. |
| `outdir` | no | Absolute path for published outputs. Empty means `/workspace/outputs/qlin/<run tag>`. |

All three are **pod paths**, not S3 URIs. `/workspace` is the FSx-Lustre filesystem;
its `/inputs` and `/reference` prefixes are auto-imported from S3 and `/outputs` is
auto-exported back, so anything published under `outdir` reaches S3 without an
explicit copy.

Everything else — reference genome, `somalier_sites`, QC thresholds, `cohort_mode` —
is run-invariant and lives in the `nextflow-qc-params` ConfigMap, alongside
`nextflow-qc-cfg` for the executor and per-process resources. Both are owned by the
kustomization in `qlin-qa-infra/kubernetes-manifests/apps/nextflow/`.

They are a **separate pair** from the post-processing `nextflow-cfg` / `nextflow-params`,
and not by preference: the post-processing config references `params.save_genotyped`
and `params.tools`, and a param referenced from a `-c` config but absent from the
`-params-file` kills the run at config parse.

## Samplesheet

| Column | Required | Notes |
|---|---|---|
| `participant` | yes | Groups samples from the same person across sequencing types. |
| `sample` | yes | Output directory name (`reports/QC/{sample}`). Unique per participant + strategy. |
| `fileType` | yes | `FASTQ`, `BAM`, `CRAM`, `VCF` or `GVCF` (case-insensitive). |
| `file1` | yes | FASTQ R1, BAM, CRAM, or VCF/GVCF. |
| `file2` | no | FASTQ R2, or the index (`.bai`/`.crai`/`.tbi`/`.csi`). Requires `file1`. |
| `familyId` | no | Needed when `cohort_mode` is false (the default) or with a `ped_file`. |
| `experimentalStrategy` | no | `WGS` (default), `WXS`, `TARS`, `RNAS`, `ATACS`, `BIS`, `TMS`, `CHIPS`. |
| `sex` | no | `Female`, `Male`, `Other`, `NA` (default). |
| `status` | no | `0` normal (default), `1` tumor. |
| `relationship_to_proband` | no | `Proband` (default), `Mother`, `Father`, … Used to derive a pedigree. |
| `affected_status` | no | `Affected`, `Unaffected`, `Unknown` (default). |
| `lane`, `runId` | no | Rows sharing participant + sample + strategy are merged before QC. |

The schema marks `file1`/`file2` as `exists: true`, so **every path is stat'd in the
driver pod at launch**. A wrong path fails the run in seconds rather than hours in —
but it also means the paths must be visible on the FSx mount, not S3 URIs.

## `dragen_metrics_dir` fails open

The directory is globbed at any depth; both `<sample>.<type>.csv` and
`<sample>.final.<type>.csv` are recognised, for `mapping_metrics`,
`wgs_coverage_metrics`, `vc_metrics` and `ploidy_estimation_metrics` (plus paired
`*_cov_report.bed` / `*_read_cov_report.bed` for per-gene coverage).

Files are matched to samplesheet rows **by sample prefix**, and a file matching no row
is **silently ignored**. So a mistyped sample name, or a metrics dir from the wrong
batch, produces a run that succeeds with a half-empty report rather than an error.

Check the report rather than the task status: every sample in the samplesheet should
have populated alignment and coverage sections. The driver log also lists what the
DRAGEN channels picked up.

## Retries and resume

The Nextflow launch directory is `/workspace/work/.nextflow-launchdir/qc-<run id>`.
The `qc-` prefix matters: `RUN_TAG` drives the work dir, the launch dir and the default
outdir, Airflow run ids are only unique *within* a DAG, and this DAG can run at the
same time as `radiant-nextflow-postprocessing`. Without the prefix the two could share
a launch dir — which is how a resume cache gets corrupted — and either `cleanup_work`
could delete the other's scratch.

The tag is stable across task retries and unique per DAG run, so:

- **A retry resumes.** `-resume` finds the previous session's `.nextflow/cache` and
  skips completed processes.
- **Concurrent runs cannot collide**, and `max_active_runs=1` keeps them from trying.

The task is **deferrable**: it releases its worker slot and the triggerer polls the
pod, so a long run costs no Airflow capacity. Logs are flushed to the task log roughly
every 10 minutes rather than streamed live.

## Before clearing a failed task

Clearing a deferred task does **not** delete its pod. If the driver is still running,
a retry starts a second driver against the same launch directory, and two Nextflow
sessions sharing one resume cache is how that cache gets corrupted. Check first:

```sh
kubectl -n nextflow get pods -l dag_id=radiant-nextflow-quality-control
kubectl -n nextflow delete pod <driver-pod>   # if one is still Running
```

`active_deadline_seconds` (24h by default) is the backstop if nobody does.

## Failure triage

A failed driver pod is kept rather than deleted, so its logs survive:

```sh
kubectl -n nextflow logs <driver-pod> --tail=200
```

The full Nextflow log, including per-process work directories, is on the shared
filesystem at `<launch dir>/.nextflow.log`. Each failed process reports its own work
directory — `.command.err` and `.command.log` there are usually more informative than
the driver's summary.

## The pipeline runs from the shared filesystem, not the image

The driver copies the pipeline out of its own image to
`/workspace/pipelines/quality-control-pipeline-<commit>` and runs *that*.

It is not tidiness. This pipeline ships nf-core module resource scripts —
`modules/local/multiqc_python/resources/usr/bin/multiqc_report.py`. Nextflow puts them
on a task's PATH by exporting the **projectDir** path into the task wrapper, but the
task runs in the module's own container (`biocontainers/multiqc`), which mounts only
`/workspace` and has no `/opt/nextflow`. Run from the image path, `MULTIQC_PYTHON`
dies with `multiqc_report.py: command not found` (exit 127). Post-processing never hit
this because it has no module resource scripts.

The copy is keyed by the asset's git commit, so a rebuilt image with a new pipeline
revision lands in a new directory rather than silently reusing a stale one. It is
~2 MB per revision, and the work-cleanup CronJob does not sweep `pipelines/` — so old
revisions accumulate slowly and can be deleted by hand if they ever matter.

Because the project path is part of each task's hash, the first run after a revision
bump re-executes rather than resuming. That is correct: the pipeline code changed.

Three failure modes are specific to this pipeline:

- **`<script>.py: command not found` (exit 127) in a module task.** The projectDir the
  worker was given is not on the shared filesystem — see above.

- **Config parse dies on an unknown `params.*` attribute.** A `withName:` block in
  `nextflow-qc-cfg` references a param that `nextflow-qc-params` does not declare. Add
  the key, or drop the reference.
- **An output lands in S3 as ~80 bytes.** A `publishDir` fell back to `symlink`, and
  the FSx auto-export does not dereference symlinks. Check that `nextflow-qc-cfg` sets
  `params.publish_dir_mode = 'copy'` and that the module in question does not use the
  `publish_mode:` key, which Nextflow silently ignores.
