# CNV post-processing: running the Ferlab `cnv-post-processing` pipeline from cases

> Status: implemented (branch `feat/cnv-post-processing`), awaiting the prerequisites in §7 for a first QA run.
> Rename this file to `design/SJRA-<ticket>-…` once the work has a ticket.

## 1. Problem

`Ferlab-Ste-Justine/cnv-post-processing` on `feat/BIOINFO-214-expand-CNV-post-processing` is no
longer the single-step Exomiser wrapper its README (and its only release, v1.0.0) describe. It is a
family-level CNV post-processing pipeline, built deliberately as the CNV sibling of
`Post-processing-Pipeline` v3.0.0:

```
per-sample DRAGEN CNV VCF ──► NORMALIZE_CNV (drop ALT=".", sort, split multiallelic)
                          ──► TRUVARI_COLLAPSE (per sample) ──► sort/index
        [group by familyId] ──► BCFTOOLS_MERGE ──► TRUVARI_COLLAPSE (cohort) ──► sort/index
                          ──► BAM_VCF_DEPTH_GENOTYPE_REFINEMENT (mosdepth; needs every member's CRAM)
                          ──► VEP (CSQ) ──► EXOMISER (family mode, needs familyPheno)
                                        └─► SLIVAR_EXPR (MoI tags, needs familyPed)
```

Radiant already runs the SNV sibling from the clinical model through two DAGs (SJRA-1843,
SJRA-1698) and the QC pipeline the same way (SJRA-1879). Nothing runs the CNV pipeline, and the
germline CNVs the portal serves today are DRAGEN's raw per-sample calls, ingested straight from the
alignment task (`gcnv`/`vcf` on `alignment_germline_variant_calling`).

## 2. Decision: a third DAG pair, sharing infrastructure with the other two

The QC integration's reasoning (SJRA-1879 §2.2) applies unchanged: folding a second pipeline into
the annotation DAG would split the single eligibility statement, couple unrelated failures, and
force two registrations into one run. So: a launcher DAG (`radiant-nextflow-cnv-postprocessing`),
a cases DAG (`radiant-nextflow-cnv-postprocessing-cases`), a sibling package
`radiant/tasks/nextflow/cnv/`, a sibling discovery template, a third launcher class and ConfigMap
pair, a third pipeline baked into the launcher image. No change to the existing DAGs.

### 2.1 Scope decided (2026-09-18)

- **Run + register only.** Outputs are registered on the cases; the variant ETL is untouched.
  Ingesting the post-processed CNV VCF into `germline_cnv_occurrence` is a separate design (§8).
- **Two new portal task types, mirroring SNV:** `radiant_germline_cnv_annotation` (multi-aliquot,
  the slivar VCF) and `exomiser_cnv` (single-aliquot, the Exomiser reports). Not the existing
  `exomiser` type: `import_part` ingests every `variants.tsv` of an `exomiser` task into the SNV
  Exomiser table, and a CNV report there would be wrong data, not surplus.
- **CRAMs are passed** for depth refinement when every member of a family has one with its index
  registered at `<cram>.crai`; otherwise the family runs without.

### 2.2 Reused as-is

`paths.py` (with `inputs_subdir="cnv-runs"`, `outputs_subdir="cnv"`), `portal.py`, `register.py`,
`case_phenotypes_select.sql`, `_nextflow_driver_operator` / `_nextflow_cleanup_operator` /
`_nextflow_image`, and — through `CnvFamily(Family)` / `CnvMember(CaseMember)` — the PED and
phenopacket writers and `Family.proband/father/mother`. The `REL_ORDER`, `SEQUENCING_TYPES`,
`ExcludedCase`, `CaseResolutionError`, `MissingOutputsError`, `GENOME_BUILD` and
`EXOMISER_PIPELINE` constants come from the parent modules.

## 3. Facts about the pipeline that shaped the integration

Read off the branch at `6b9b2dd` (no PR, no tag; `nextflowVersion '!>=23.10.1'`, `nf-schema@2.3.0`).

| Fact | Consequence |
|---|---|
| Samplesheet `familyId,sample,sequencingType,caller,vcf,cram,pheno,familyPheno,familyPed`; `caller` ∈ `DRAGEN\|DRAGEN_JOINT\|GATK` but `NORMALIZE_CNV` errors on anything but `DRAGEN*`. | `caller` is the constant `DRAGEN`; the query returns the alignment's `pipeline_name` and `resolve` excludes `unsupported_caller` (not DRAGEN) and `unknown_caller` (null `pipeline_name`, which is nullable) before anything is staged — it fails closed rather than asserting DRAGEN. |
| `cram` index resolved as `${cram}.crai` **by file existence on the mount**; no index → mosdepth fails (not skips). A family is refined only if *every* sample has a CRAM. | `CnvFamily.crams_complete` is all-or-nothing and also requires `crai_url == cram_url + ".crai"`. |
| A family with `familyPed`/`familyPheno` takes the family route even at `sampleSize == 1` ("documented singleton"); the per-sample `pheno` column selects the solo route, whose outputs are named `<familyId>.<sample>`. | Every row carries `familyPed` + `familyPheno`, `pheno` is always empty, so every output is `CA<case id>`-keyed. |
| `bin/refine_genotypes.py` is a module script called by name from a pysam container that mounts only the workspace. | The driver copies the project to `${NXF_WORKSPACE}/pipelines/cnv-post-processing-<rev>` and runs it from there — the QC driver's fix, now shared through `_shared_project_driver_script`. |
| `SLIVAR_EXPR` publishes `slivar/<familyId>.cnv.slivar.vcf.gz` **without an index**; the SNV sibling publishes vcf + tbi. | `OUTPUT_SPEC` registers the VCF only. A `slivar_tbi` entry is added in the same change that bumps `CNV_PIPELINE_REV` to a revision that indexes it (§7.1) — expecting one earlier would fail every run in `collect_outputs`. |
| `exomiser_start_from_vep` defaults to false: Exomiser reads the depth-refined family VCF, not the VEP one. Neither is published as a document. | Same TASK-005 compromise as SNV: `exomiser_cnv`'s input is recorded as the slivar VCF. |
| Containers: `quay.io/biocontainers/*`, `docker.io/brentp/slivar:v0.3.1`, `docker.io/ensemblorg/ensembl-vep:release_114.2`, `registry.hub.docker.com/ferlabcrsj/exomiser:2.8.1`. `process_medium` = 6 cpu / 36 GB; EXOMISER disk 200 GB + 50 GB/attempt. | Infra checks in §7.3. |

## 4. The DAGs

```
radiant-nextflow-cnv-postprocessing-cases (@daily, max_active_runs=1)

discover_scope -> select_cases -> fetch_phenotypes -> resolve_cases -> generate_inputs
                                                                              |
                                                                        run_pipeline  ──► radiant-nextflow-cnv-postprocessing
                                                                              |            run_cnv_postprocessing >> cleanup_work
          register_tasks (one per tenant) <- collect_outputs <----------------+
```

- `discover_scope` — `sql/clinical/pending_cnv_annotation_select.sql`: one row per (candidate
  germline case, member). `germline_case`, `current_experiment`, `current_alignment` are the
  annotation query's, byte for byte (pinned by
  `test_the_supersession_ctes_are_identical_to_the_annotation_query`). The trigger is the
  alignment's `gcnv`/`vcf` counted over one task (`no_gcnv`, `ambiguous_gcnv`); `cram`/`crai` ride
  along; the anti-join is on `radiant_germline_cnv_annotation` scoped to `(case, seq)`; the
  alignment task's `pipeline_name` comes back as `alignment_pipeline`.
- `select_cases` — `cnv.resolve.select_cases`: strict when `task_ids` were named, lenient
  otherwise; adds `unsupported_caller` / `unknown_caller` to the query's reasons; excludes at case granularity.
- `fetch_phenotypes` / `resolve_cases` — unchanged query, `cnv.resolve.resolve_families` →
  `CnvFamily`.
- `generate_inputs` — `cnv.inputs.build_inputs`: 9-column samplesheet + `pedigrees/CA<id>.ped` +
  `phenotypes/CA<id>.yml` under `{NEXTFLOW_INPUTS_ROOT}/cnv-runs/{run_tag}/`, clearing only that
  prefix first.
- `run_pipeline` — `TriggerDagRunOperator`, `trigger_run_id` pinned to the run tag so a retry
  `-resume`s; the launcher prefixes `cnv-` on the shared filesystem.
- `collect_outputs` — polls `{NEXTFLOW_OUTPUTS_ROOT}/cnv/{run_tag}/` for the complete
  `OUTPUT_SPEC` set of every family; a partial run registers nothing.
- `register_tasks` — mapped per tenant, `cnv.batch.build_patch_body` → `register_case_batch`.

Launcher: `NextflowCnvPostprocessing` in `radiant/dags/operators/k8s.py`, ConfigMaps
`nextflow-cnv-cfg` / `nextflow-cnv-params` (env-overridable), image
`NEXTFLOW_CNV_OPERATOR_IMAGE` falling back to `NEXTFLOW_OPERATOR_IMAGE`. Dockerfile: `CNV_PIPELINE_REPO` /
`CNV_PIPELINE_REV` pulled with `-r` (manifest `defaultBranch = 'master'`), sanity `ls` on the asset.

## 5. What gets registered

Two tasks per case through `PATCH /{tenant}/cases/batch`:

| Task | Aliquots | `input_documents` | `output_documents` |
|---|---|---|---|
| `radiant_germline_cnv_annotation` (`cnv-post-processing` @ `CNV_PIPELINE_REV`) | all members | each member's `gcnv` VCF, plus CRAM + crai when `crams_complete` | `slivar/CA<id>.cnv.slivar.vcf.gz` → `gcnv`/`vcf` |
| `exomiser_cnv` (`Exomiser` @ `14.0.0`) | proband | the slivar VCF above (in-batch) | `exomiser/CA<id>.exomiser.{variants.tsv,html,json}` → `exomiser`/`tsv`,`html`,`json` |

**ETL visibility.** `staging_external_sequencing_experiment` admits `gcnv`/`vcf` only on an
`alignment_germline_variant_calling` task, and `variants.tsv` only on an `exomiser` task. Both new
task types match none of its predicates: the documents are served by the portal and invisible to
the variant ETL. Intended for this phase.

## 6. Verification

- `make test-static`; `pytest tests/unit` — 1046 passed (2026-09-18), including
  `tests/unit/nextflow/cnv/`, `test_nextflow_cnv_postprocessing*.py`,
  `test_clinical_cnv_sql_render.py`, `test_nextflow_cnv_postprocessing_k8s_operator.py`.
- `USE_DOCKER_FIXTURES=true pytest -m "not slow" tests/integration/dags/sql/test_cnv_case_resolution.py`
  — case 16 (the seeded trio, now with `gcnv` documents 268–273 on alignments 44/45/46) and case 8
  (SNV-annotated singleton whose alignment 22 also published a `gcnv` VCF) are the pending cases;
  case 1 (task 72, `radiant_germline_cnv_annotation`) is not; `task_ids=[44]` returns the whole
  trio. Run against the Docker fixtures on 2026-09-18 (29 passed across the three case-resolution
  suites; the one failure was a wrong expectation about case 8 in the new test, since corrected). Note the QC query now yields six document rows for task 44
  (`test_qc_case_resolution.py` updated).
- Launcher image: `docker build -f Dockerfile.nextflow.launcher .` passes its sanity `RUN` with the
  third asset present (built locally 2026-09-18; `cnv-post-processing` pulled at `6b9b2dd15ce…`).
- QA, once §7 is done: `dry_run=true` with one trio's `task_ids`; inspect the logged samplesheet
  (9 columns, `caller=DRAGEN`, mount paths, `cram` filled or empty as a whole), the driver log
  (`>> pipeline rev=… project=/workspace/pipelines/cnv-post-processing-…`), `collect_outputs`, the
  dry-run PATCH report; then a real run, the two tasks on the case in the portal, and the next
  nightly no longer proposing it.

## 7. Prerequisites outside this repository

### 7.1 Pipeline (`cnv-post-processing`)
- Publish a `.tbi` next to `slivar/<familyId>.cnv.slivar.vcf.gz` (a `BCFTOOLS_INDEX` after
  `SLIVAR_EXPR`, or `tabix` in the module). When the pin is bumped to such a revision, add a
  `slivar_tbi` entry to `OUTPUT_SPEC` / `ANNOTATION_OUTPUTS` in the same change.
- Tag a release, then set `CNV_PIPELINE_REV` and `cnv.batch.ANNOTATION_PIPELINE` to it together.
- Optional: name the final VCF `variants.<familyId>.cnv.vep.slivar.vcf.gz` to match the SNV
  convention; update `OUTPUT_SPEC` if so.

### 7.2 Portal
- Task type codes `radiant_germline_cnv_annotation` and `exomiser_cnv`; `exomiser_cnv` in
  `SingleAliquotTaskTypes`, both in `RequiresInputDocumentsTaskTypes`. Same service account and
  `ingest_data` grant as the other registrations.

### 7.3 Infrastructure (`qlin-qa-infra`)
- ConfigMap `nextflow-cnv-cfg`: k8s executor, `workDir` keyed by `RUN_TAG`, `publish_dir_mode = 'copy'`,
  container overrides if `docker.io` / `quay.io` are not reachable from the `nextflow` namespace.
- ConfigMap `nextflow-cnv-params`: `reference_fasta` (+ `.fai`) on FSx; `vep_cache`,
  `vep_cache_version = 114`, `vep_genome = GRCh38`, `vep_annotation = merged` (the SNV cache);
  `exomiser_data_dir`, `exomiser_data_version = 2402`, `exomiser_genome = hg38`;
  `exomiser_start_from_vep` as the team decides.
- Airflow env: `NEXTFLOW_CNV_TENANTS` (optional; falls back to `NEXTFLOW_POSTPROCESSING_TENANTS`).
- Rebuild and push the launcher image; confirm the `qlin-nextflow` nodepool schedules
  `process_medium` (6 cpu / 36 GB) and Exomiser's 250 GB ephemeral disk request.

## 8. Out of scope

- **Ingesting the post-processed CNV VCF.** Known work: a WHERE line in
  `staging_external_sequencing_experiment` for (`radiant_germline_cnv_annotation`, `gcnv`, `vcf`);
  a task model in `radiant/tasks/vcf/experiment.py` (the alignment one hard-fails on more than one
  row per task); per-sample genotype filtering in `radiant/tasks/vcf/cnv/germline/occurrence.py`
  (the multi-sample family VCF carries `0/0` and `./.` rows the current extractor would emit as
  occurrences); columns for VEP `CSQ` and the six slivar MoI INFO tags.
- **Joint-called (`DRAGEN_JOINT`) families.** Cases with no per-member `gcnv` document are not
  candidates, as for SNV.
- **`nextflow_postprocessing_cases.py`** keeps its inline registration copy.

## Annex A. Worked example: what one case registers

Trio, tenant `radiant`, case `16` (`submitter_case_id` and project from the seeds), members with
experiments 44/45/46 and aliquots `S14744`/`S14745`/`S14746`, run tag
`scheduled-2026-09-18T00-00-00-00-00`, every member with a CRAM whose index is at `<cram>.crai`.

```json
{
  "cases": [
    {
      "project_code": "P01",
      "submitter_case_id": "CA-0016",
      "tasks": [
        {
          "type_code": "radiant_germline_cnv_annotation",
          "aliquots": ["S14744", "S14745", "S14746"],
          "pipeline_name": "cnv-post-processing",
          "pipeline_version": "6b9b2dd",
          "genome_build": "GRch38",
          "input_documents": [
            {"url": "s3://…/FI0037789.S14744.cnv.vcf.gz"},
            {"url": "s3://…/FI0037798.S14745.cnv.vcf.gz"},
            {"url": "s3://…/FI0037690.S14746.cnv.vcf.gz"},
            {"url": "s3://…/FI0037789.S14744.cram"}, {"url": "s3://…/FI0037789.S14744.cram.crai"},
            {"url": "s3://…/FI0037798.S14745.cram"}, {"url": "s3://…/FI0037798.S14745.cram.crai"},
            {"url": "s3://…/FI0037690.S14746.cram"}, {"url": "s3://…/FI0037690.S14746.cram.crai"}
          ],
          "output_documents": [
            {"name": "CA16.cnv.slivar.vcf.gz", "url": "s3://…/cnv/scheduled-2026-09-18T00-00-00-00-00/slivar/CA16.cnv.slivar.vcf.gz", "size": 812345, "data_category_code": "genomic", "data_type_code": "gcnv", "format_code": "vcf"}
          ]
        },
        {
          "type_code": "exomiser_cnv",
          "aliquots": ["S14744"],
          "pipeline_name": "Exomiser",
          "pipeline_version": "14.0.0",
          "genome_build": "GRch38",
          "input_documents": [
            {"url": "s3://…/cnv/scheduled-2026-09-18T00-00-00-00-00/slivar/CA16.cnv.slivar.vcf.gz"}
          ],
          "output_documents": [
            {"name": "CA16.exomiser.variants.tsv", "url": "s3://…/exomiser/CA16.exomiser.variants.tsv", "size": 31230, "data_category_code": "genomic", "data_type_code": "exomiser", "format_code": "tsv"},
            {"name": "CA16.exomiser.html",         "url": "s3://…/exomiser/CA16.exomiser.html",         "size": 912000, "data_category_code": "genomic", "data_type_code": "exomiser", "format_code": "html"},
            {"name": "CA16.exomiser.json",         "url": "s3://…/exomiser/CA16.exomiser.json",         "size": 402000, "data_category_code": "genomic", "data_type_code": "exomiser", "format_code": "json"}
          ]
        }
      ]
    }
  ]
}
```

The portal writes one `task` row per task type (both with `case_id` set in `task_context`, one row
per experiment — which is what the anti-join reads), one `document` row per output, and
`task_has_document` rows: outputs to the new documents, inputs to the alignment's existing
`gcnv`/CRAM/crai documents (task 1) and to the new slivar VCF (task 2). The next nightly
`discover_scope` sees `radiant_germline_cnv_annotation` on `(16, 44)`, `(16, 45)`, `(16, 46)` and
stops proposing case 16; a re-sequenced member gets a new experiment id and only that case comes
back. PATCH appends: a deliberate re-run adds a second pair of tasks alongside the first.
