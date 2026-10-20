# Nextflow Somatic SNV Annotation

Annotates the raw VCFs of somatic calling tasks with the Ferlab
[snv-post-processing](https://github.com/Ferlab-Ste-Justine/snv-post-processing), the germline
pipeline, entered at its **normalize** step with **tools = vep**: split multi-allelic records,
left-align indels on the reference, then VEP. No genotyping, slivar or Exomiser.

Started by **radiant-nextflow-somatic-snv-annotation-cases**, which writes the samplesheet and
registers the outputs. Run it by hand only to test a samplesheet.

---

## Parameters

| Param | Required | Meaning |
|:--|:--|:--|
| **input** | Yes | Absolute path to the samplesheet CSV on the shared workspace |
| **outdir** | No | Absolute path for published outputs. Empty means /workspace/outputs/qlin/ plus the run tag |

Samplesheet columns: familyId (the calling task id), sequencingType, vcf. Both params are **pod
paths**, not S3 URIs.

| ConfigMap | Holds |
|:--|:--|
| **nextflow-snv-somatic-params** | step normalize, tools vep, the germline reference and VEP values |
| **nextflow-snv-cfg** | The executor and per-process resources, shared with the germline launcher |

Without **nextflow-snv-somatic-params** the driver pod does not start.

---

## Why a DAG of its own

Same pipeline, image and **nextflow-snv-cfg** as **radiant-nextflow-snv-postprocessing**, so the
same VEP image, cache and flags. A separate DAG, because each launcher runs one driver at a time: on
the germline one, a somatic run would wait behind hours of germline WGS, and the scheduled import
behind both. Two drivers at once are safe: the launch and work directories are keyed by the run
tag, which the cases DAG prefixes with **somatic-**.

Retries, resume, clearing a failed task and triage work exactly as for
**radiant-nextflow-snv-postprocessing**: see its documentation.
