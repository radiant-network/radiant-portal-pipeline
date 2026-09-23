# Nextflow CNV Post-processing

Runs the Ferlab [cnv-post-processing](https://github.com/Ferlab-Ste-Justine/cnv-post-processing)
pipeline on qlin-eks over germline CNV VCFs: per-sample normalization and truvari
collapse, family merge and cohort collapse, depth-based genotype refinement (mosdepth,
when CRAMs are given), VEP annotation, Exomiser prioritization and slivar
mode-of-inheritance classification.

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
| `outdir` | no | Absolute path for published outputs. Empty means `/workspace/outputs/qlin/<run tag>`. |

Both are **pod paths**, not S3 URIs. `/workspace` is the FSx-Lustre filesystem; its
`/inputs` and `/reference` prefixes are auto-imported from S3 and `/outputs` is
auto-exported back.

Everything else — `reference_fasta`, the VEP cache (`vep_cache`, `vep_cache_version`,
`vep_genome`, `vep_annotation`), the Exomiser data (`exomiser_data_dir`,
`exomiser_data_version`, `exomiser_genome`), `exomiser_start_from_vep` — is run-invariant
and lives in the `nextflow-cnv-params` ConfigMap, alongside `nextflow-cnv-cfg` for the
executor and per-process resources. Both are owned by the kustomization in
`qlin-qa-infra/kubernetes-manifests/apps/nextflow/`.

They are a **separate pair** from the other two launchers' ConfigMaps, for the same
reason those two are separate: a param referenced from a `-c` config but absent from the
`-params-file` kills the run at config parse.

## Samplesheet

| Column | Required | Notes |
|---|---|---|
| `familyId` | yes | Groups rows into a family; names every family-level output. |
| `sample` | yes | The aliquot, which is the sample name in the VCF. Not the submitter sample id. |
| `sequencingType` | yes | `WGS` or `WES`; selects the Exomiser analysis file. |
| `caller` | yes | `DRAGEN` (per-sample VCF) or `DRAGEN_JOINT` (one family-level VCF). Only DRAGEN conventions are implemented. |
| `vcf` | yes | The CNV VCF (`.vcf` / `.vcf.gz`). |
| `cram` | no | Alignment for depth-based genotype refinement. Its index must exist at `<cram>.crai` (or `.bai`) — resolved by file existence. |
| `pheno` | no | Per-sample phenopacket; selects the *solo* route. Left empty by the cases DAG. |
| `familyPheno` | no | Family phenopacket (proband + pedigree); enables Exomiser (family mode). Identical across a family's rows. |
| `familyPed` | no | PED file; enables slivar. Identical across a family's rows. |

The schema marks `vcf`, `cram` and the phenotype files as `exists: true`, so **every path
is stat'd in the driver pod at launch**.

## Outputs

Per family, under `outdir`:

| Path | What |
|---|---|
| `slivar/<familyId>.cnv.slivar.vcf.gz` | The final VCF: VEP `CSQ` plus one INFO tag per mode of inheritance (`de_novo_candidate`, `dominant_inherited`, `recessive_candidate`, `candidate`, `ambiguous`, `unknown_cn`). |
| `ensemblvep/variants.<familyId>.cnv.vep.vcf.gz{,.tbi}` | The VEP-annotated family VCF. |
| `exomiser/<familyId>.exomiser.{variants.tsv,genes.tsv,html,json,vcf.gz,vcf.gz.tbi}` | Exomiser (family mode). |
| `merged/`, `truvari/`, `depth_refined/`, `joint/` | Intermediates. |

## Retries and resume

The Nextflow launch directory is `/workspace/work/.nextflow-launchdir/cnv-<run id>`.
The `cnv-` prefix matters: `RUN_TAG` drives the work dir, the launch dir and the default
outdir, Airflow run ids are only unique *within* a DAG, and this DAG can run at the same
time as the other two launchers. Without the prefix two runs could share a launch dir —
which is how a resume cache gets corrupted — and either `cleanup_work` could delete the
other's scratch.

The tag is stable across task retries and unique per DAG run, so a retry `-resume`s: it
finds the previous session's `.nextflow/cache` and skips completed processes.

The task is **deferrable**: it releases its worker slot and the triggerer polls the pod.
Logs are flushed to the task log roughly every 10 minutes rather than streamed live.

## Before clearing a failed task

Clearing a deferred task does **not** delete its pod. If the driver is still running, a
retry starts a second driver against the same launch directory. Check first:

```sh
kubectl -n nextflow get pods -l dag_id=radiant-nextflow-cnv-postprocessing
kubectl -n nextflow delete pod <driver-pod>   # if one is still Running
```

`active_deadline_seconds` (24h by default) is the backstop if nobody does.

## The pipeline runs from the shared filesystem, not the image

The driver copies the pipeline out of its own image to
`/workspace/pipelines/cnv-post-processing-<commit>` and runs *that*, exactly as the QC
launcher does.

It is not tidiness. This pipeline ships `bin/refine_genotypes.py`, which the
`DEPTH_GENOTYPE_REFINE` process calls by name. Nextflow puts `projectDir/bin` on a task's
PATH by exporting the **projectDir** path into the task wrapper, but the task runs in the
module's own container (`biocontainers/pysam`), which mounts only `/workspace` and has no
`/opt/nextflow`. Run from the image path, the process dies with
`refine_genotypes.py: command not found` (exit 127).

Because the project path is part of each task's hash, the first run after a revision bump
re-executes rather than resuming. That is correct: the pipeline code changed.

## Failure triage

A failed driver pod is kept rather than deleted, so its logs survive:

```sh
kubectl -n nextflow logs <driver-pod> --tail=200
```

The full Nextflow log is on the shared filesystem at `<launch dir>/.nextflow.log`. Each
failed process reports its own work directory — `.command.err` and `.command.log` there
are usually more informative than the driver's summary.

Failure modes specific to this pipeline:

- **`NORMALIZE_CNV: caller '...' is not yet supported`.** A samplesheet row's `caller` is
  not `DRAGEN` / `DRAGEN_JOINT`. The cases DAG only ever writes `DRAGEN`, and excludes
  cases whose alignment was not produced by DRAGEN.
- **mosdepth fails to open a CRAM.** The index is not at `<cram>.crai` on the mount. The
  cases DAG only passes CRAMs when every member's index is registered at that path.
- **Exomiser `ClassCastException` on `SVLEN`.** A jointly-called DRAGEN VCF fed as
  `caller=DRAGEN`; use `DRAGEN_JOINT`, which strips `INFO/SVLEN`.
- **An image cannot be pulled.** The pipeline pins `quay.io/biocontainers/*`,
  `docker.io/brentp/slivar`, `docker.io/ensemblorg/ensembl-vep` and
  `registry.hub.docker.com/ferlabcrsj/exomiser`. Override the container in
  `nextflow-cnv-cfg` if a registry is not reachable from the cluster.
- **Config parse dies on an unknown `params.*` attribute.** A `withName:` block in
  `nextflow-cnv-cfg` references a param that `nextflow-cnv-params` does not declare.
