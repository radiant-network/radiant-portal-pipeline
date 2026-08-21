# Nextflow Post-processing

Runs the Ferlab [Post-processing-Pipeline](https://github.com/Ferlab-Ste-Justine/Post-processing-Pipeline)
on qlin-eks: joint genotyping from per-sample gVCFs, then VEP → slivar → Exomiser.

One Airflow task launches a Nextflow **driver** pod in the `nextflow` namespace. The
driver spawns one worker pod per pipeline process itself, through Nextflow's
Kubernetes executor — so a single task in the Airflow UI is a fan-out of dozens of
pods in Kubernetes. Watch them with:

```sh
kubectl -n nextflow get pods -w
```

## Parameters

| Param | Required | Meaning |
|---|---|---|
| `input` | yes | Absolute path to the samplesheet CSV on the shared workspace, e.g. `/workspace/inputs/1000genomes-dragen-v4-4-7/samplesheet.csv`. Columns: `familyId,sample,sequencingType,gvcf,familyPheno,familyPed`. |
| `outdir` | no | Absolute path for published outputs. Empty means `/workspace/outputs/qlin/<run tag>`. |

Both paths are **pod paths**, not S3 URIs. `/workspace` is the FSx-Lustre filesystem;
its `/inputs` and `/reference` prefixes are auto-imported from S3 and `/outputs` is
auto-exported back, so anything published under `outdir` reaches
`s3://qlin-qa-nextflow-outputs-*` without an explicit copy.

Everything else — reference genome, VEP cache, Exomiser data, `tools`, `step` — is
run-invariant and lives in the `nextflow-params` ConfigMap, alongside `nextflow-cfg`
for the executor and per-process resources. Both are owned by the kustomization in
`qlin-qa-infra/kubernetes-manifests/apps/nextflow/`.

## Retries and resume

The Nextflow launch directory is `/workspace/work/.nextflow-launchdir/<run tag>`,
where the run tag comes from the Airflow `run_id`. It is stable across task retries
and unique per DAG run, so:

- **A retry resumes.** `-resume` finds the previous session's `.nextflow/cache` and
  skips completed processes. On WGS that is the difference between minutes and hours.
- **Concurrent runs cannot collide**, and `max_active_runs=1` keeps them from trying.

The task is **deferrable**: it releases its worker slot and the triggerer polls the
pod, so a multi-hour run costs no Airflow capacity. Logs are flushed to the task log
roughly every 10 minutes rather than streamed live.

## Before clearing a failed task

Clearing a deferred task does **not** delete its pod. If the driver is still running,
a retry starts a second driver against the same launch directory, and two Nextflow
sessions sharing one resume cache is how that cache gets corrupted. Check first:

```sh
kubectl -n nextflow get pods -l dag_id=radiant-nextflow-postprocessing
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
