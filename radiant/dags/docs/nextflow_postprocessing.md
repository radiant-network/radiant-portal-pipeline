# Nextflow Post-processing

Runs the Ferlab
[Post-processing-Pipeline](https://github.com/Ferlab-Ste-Justine/Post-processing-Pipeline)
on qlin-eks: joint genotyping from per-sample gVCFs, then VEP, slivar and Exomiser.

> **One Airflow task is a fan-out of dozens of pods.** The task launches a Nextflow
> **driver** pod in the **nextflow** namespace, and the driver then spawns one worker pod
> per pipeline process itself, through Nextflow's Kubernetes executor. Airflow sees one
> task; Kubernetes sees the whole fan-out.

Watch them with **kubectl -n nextflow get pods -w**.

---

## Parameters

| Param | Required | Meaning |
|:--|:--|:--|
| **input** | Yes | Absolute path to the samplesheet CSV on the shared workspace |
| **outdir** | No | Absolute path for published outputs. Empty means /workspace/outputs/qlin/ plus the run tag |

Samplesheet columns: familyId, sample, sequencingType, gvcf, familyPheno, familyPed. An
example input path is /workspace/inputs/1000genomes-dragen-v4-4-7/samplesheet.csv.

> Both are **pod paths**, not S3 URIs.

/workspace is the FSx-Lustre filesystem. Its /inputs and /reference prefixes are
auto-imported from S3, and /outputs is auto-exported back, so anything published under
**outdir** reaches the qlin-qa-nextflow-outputs bucket without an explicit copy.

Everything else is run-invariant and lives in ConfigMaps rather than params:

| ConfigMap | Holds |
|:--|:--|
| **nextflow-params** | Reference genome, VEP cache, Exomiser data, tools, step |
| **nextflow-cfg** | The executor and per-process resources |

Both are owned by the kustomization in qlin-qa-infra/kubernetes-manifests/apps/nextflow/.

---

## Retries and resume

The Nextflow launch directory is /workspace/work/.nextflow-launchdir/ plus the run tag,
where the tag comes from the Airflow **run_id**. It is stable across task retries and
unique per DAG run, so:

- **A retry resumes.** The -resume flag finds the previous session's .nextflow/cache and
  skips completed processes. On WGS that is the difference between minutes and hours.
- **Concurrent runs cannot collide**, and **max_active_runs=1** keeps them from trying.

The task is **deferrable**: it releases its worker slot and the triggerer polls the pod, so
a multi-hour run costs no Airflow capacity. Logs are flushed to the task log roughly every
10 minutes rather than streamed live.

---

## Before clearing a failed task

> **Clearing a deferred task does not delete its pod.** If the driver is still running, a
> retry starts a second driver against the same launch directory — and two Nextflow
> sessions sharing one resume cache is how that cache gets corrupted.

Check first:

| Purpose | Command |
|:--|:--|
| Is a driver still up? | **kubectl -n nextflow get pods -l dag_id=radiant-nextflow-postprocessing** |
| Kill it, if Running | **kubectl -n nextflow delete pod DRIVER_POD** |

**active_deadline_seconds** (24h by default) is the backstop if nobody does.

---

## Failure triage

A failed driver pod is kept rather than deleted, so its logs survive. Work outward:

1. **The driver's own log** — kubectl -n nextflow logs DRIVER_POD --tail=200
2. **The full Nextflow log** — .nextflow.log inside the launch directory on the shared
   filesystem, including per-process work directories.
3. **The failing process's work directory** — each failed process reports its own.
   .command.err and .command.log there are usually more informative than the driver's
   summary.
