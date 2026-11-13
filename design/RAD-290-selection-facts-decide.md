# RAD-290 Selection facts in SQL, decide() in Python

Story [RAD-286](https://ferlab-crsj.atlassian.net/browse/RAD-286), Task 4 of 6. Builds on RAD-287 (shared views) and RAD-288 (decision log). Analysis: <https://claude.ai/artifact/AEBifGKjC9oock66MxHAh3> (step 04).

**Summary:** the selection queries stop deciding. Each pipeline gets a short query that returns facts about every case (members, current experiment, current alignment, documents, what is already annotated), and a pure Python function `decide_<pipeline>` that turns these facts into the decisions of RAD-288. Two decisions become visible: `done` and "no trigger document". The same cases run and the same statuses move. Done one pipeline at a time.

## 1. Why

- A case's decision is made in two layers:
  - SQL: the `candidate` CTE and the `CASE … exclusion_reason` of the five pending queries (100 to 150 lines each);
  - Python: the reasons the selections add on top (`missing_aliquot`, `proband_count`, `unsupported_strategy`, `unknown_caller`, `unsupported_caller`, `unsupported_samples`).
- The SQL layer decides in silence. A case that is already annotated, or that has no trigger document at all, is not returned. Nothing records why.
- In the RAD-288 log, "absent at `after`" therefore means "done". An operator has to read an absence.
- The scenarios that matter (re-sequenced parent, shared experiment, ambiguous documents) can only be tested against StarRocks, with seeded data.

## 2. What exists today

**Per pipeline, the query decides three things.**

| Pipeline | Trigger document (current alignment) | Already done when | Reasons, first match wins |
|---|---|---|---|
| germline SNV | `snv` / `gvcf` | a `radiant_germline_annotation` for (case, experiment) | `pending_sequencing`, `pending_alignment`, `no_gvcf`, `ambiguous_gvcf`, `no_project_code`, `tenant_excluded` |
| germline CNV | `gcnv` / `vcf` | a `radiant_germline_cnv_annotation` for (case, experiment) | the same, with `no_gcnv`, `ambiguous_gcnv` |
| QC germline | `alignment` / `cram` | a `quality_control_metrics` for (case, experiment) | the same, with `no_cram`, `ambiguous_cram`, `ambiguous_dragen_metrics` |
| QC somatic | the tumor's current alignment | a `quality_control_metrics` for (case, tumor experiment) | `ambiguous_dragen_metrics`, `no_metrics`, `no_project_code`, `tenant_excluded` |
| somatic SNV | `ssnv` / `vcf` of the calling task | an output of the calling task is the input of a `radiant_somatic_annotation` on the case | `pending_sequencing`, `no_ssnv`, `ambiguous_ssnv`, `no_project_code`, `tenant_excluded` |

**Candidates only.** A germline case is returned when at least one member's current experiment has the trigger document and no annotation for (case, experiment). A case where no member has the document (joint-called upstream) is "deliberately not a candidate".

**Uneven edges.**
- QC somatic inner-joins the tumor and its alignment: a case without them is absent, with no `pending_*` reason.
- Somatic SNV reports `no_ssnv` on a targeted run only; on a discovery run a calling task without its VCF is absent.
- Somatic SNV does not use the current views: it reads the experiments of the calling task, and `pending_sequencing` means one of them is not `completed`.

**Supersession is already shared.** The RAD-287 views pick the current experiment of each member and the current alignment of each experiment. RAD-290 does not touch them.

**What the children need from the rows.** Besides the decision: aliquot, role, sex, affected status, strategy, the document URLs (gVCF, gCNV, CRAM, CRAI, metrics tarball), the caller (CNV), project code, submitter case id. Phenotypes come from a separate query.

## 3. Options

- **A. Fact queries, decisions in Python (chosen).** One short query per pipeline over the RAD-287 views, with no `CASE` and no `candidate` filter. A pure function decides.
  - Pros: one layer decides; every scenario is a unit test; `done` and "no trigger" come for free.
  - Cons: every case comes back each run, not only the candidates; the four queries and selections are rewritten.
- **B. Keep the `CASE` in SQL, drop only the `candidate` filter.**
  - Pros: smaller change.
  - Cons: still two layers; the scenarios stay integration tests.
- **C. Everything in Python, supersession included.** Read the raw clinical tables.
  - Pros: no SQL logic at all.
  - Cons: undoes RAD-287; the supersession rules would exist twice (the import's staging view keeps them in SQL).

## 4. Design

### Facts

**One query per pipeline.** It returns every case of `pipeline_case` of its case type, one row per member, with:
- the case: id, tenant, project code, submitter case id, status;
- the member: patient, role, sex, affected status;
- its current experiment (NULL when none), aliquot, strategy;
- its current alignment (NULL when none), with the caller for CNV;
- per trigger document of that alignment: the number of matches and the URL;
- whether the (case, experiment) pair is already annotated by this pipeline.

Somatic SNV keeps its own shape: one row per (calling task, case, experiment), plus one row per somatic case that has no calling task.

**No decision in SQL.** No `CASE`, no `candidate`, no filter on `task_ids` or `excluded_tenants`. Both become inputs of `decide`.

**Typed facts.** Pydantic models (`CaseFacts`, `MemberFacts`, and a somatic `UnitFacts`) built from the rows. The children build their inputs from the same models.

### `decide_<pipeline>(facts, excluded_tenants, task_ids)`

Pure function, no I/O. Returns the RAD-288 `Decision` rows (case row + member rows). Rules, in order:

1. **Work**: a member has the trigger document and no annotation for (case, experiment). The case is a candidate: the current reasons, in their current order, then the Python ones, decide `run`, `wait`, `excluded` or `skipped`. They keep their codes.
2. **Done**: otherwise, the case is annotated for at least one member: `done`. A case annotated from a joint VCF upstream (no gVCF) is `done` too.
3. **No trigger document**: otherwise (somatic: no calling task, or a calling task without its VCF on a discovery run): `skipped`, reason `no_trigger_document`.

`task_ids` narrows candidacy as today: a case is a candidate only through a requested task. A targeted run keeps its strict mode and its `no_ssnv` error.

The selections the children call (`select_cases`, `select_tasks`) become thin adapters over `decide_<pipeline>`, so a child and the scheduler cannot decide differently.

**A child reads the decided cases only.** The fact query takes an optional `case_ids`. The scheduler's discovery passes none and gets every case. A child triggered by the scheduler first reads the cases decided `run` from the log, then the facts of those cases with every member, and decides again over them (a case no longer runnable since is dropped). Run by hand, it reads every case.

### New decision: `done`

- `done`: the pipeline has nothing left to do for the unit.
- Never work waiting: the RAD-288 consumers already count `run` only (the processing plan, the children's narrowing, the run units query).
- Nobody writes a `done` status. A child registers its output task; the scheduler's next pass sees the annotation in the facts and decides `done`.

### What is written to the log

The decisions are computed for every case on each pass. Not all are written:

- `run`, `wait`, `excluded`, `skipped` (`tenant_excluded`): written at both passes, as in RAD-288.
- `done`: written at `after` only, for the units decided `run` at `discover` of the same run. The log then shows `run` → `done` in one run. No read of the log's history.
- `skipped` / `no_trigger_document` (proposed): written only for the cases still `submitted`, where it explains why nothing happens. The scheduler already reads every case's status.

Volume: the RAD-288 estimate (a few hundred rows a day) plus one `done` row per unit a pipeline completed. No retention needed.

**Known gap.** A child run by hand completes units that no scheduler pass sees run. Their last row stays the previous decision until a later `run` → `done`.

## 5. Rewriting the consumers

**`radiant/dags/sql/clinical/`:** each `pending_*_select.sql` is replaced by a `*_facts_select.sql` over the RAD-287 views. The integration tests of a pipeline move to its fact query when it is migrated.

**`radiant/tasks/nextflow/{,cnv/,qc/,somatic/}`:**
- the fact models;
- `decide_<pipeline>`, which absorbs `_validate` and the SQL reasons;
- `select_cases` / `select_tasks` become adapters; their strict mode and their outputs (members, URLs) stay.

**`radiant/tasks/nextflow/decision.py`:**
- `DONE` and `no_trigger_document`;
- `decide` calls the four `decide_<pipeline>`;
- the write filter of §4 (it needs the `discover` run units and the case statuses at `after`).

**`case_scheduler.py`:** `discover_*` / `rediscover_*` run the fact queries.

**`case_status.py`:** unchanged.

**Docs:** `case_scheduler.md`, the four `nextflow_*_cases.md`, and the reading rules under the RAD-288 diagram ("absent at `after`" becomes `done`).

## 6. Behavior

**No change intended:**
- the same units run in each pipeline;
- the same statuses move;
- the same lab emails go out.

**What changes:**
- the log gains `done` rows at `after`, and `skipped` / `no_trigger_document` rows for `submitted` cases;
- QC somatic reports a tumor not aligned yet as `skipped` / `no_trigger_document` instead of leaving it out (its alignment is the trigger), and a somatic SNV calling task without its VCF likewise on a discovery run;
- the fact queries return every case, not only the candidates. They read the same views; the row count (cases × members) is to check on QA.

## 7. Deployment

- No migration. `decision` is a VARCHAR; the DDL comment of `pipeline_decision_log` gains `done`.
- Two PRs (§11). The first deploys alone: the other pipelines keep their pending query until the second.
- Rollback: deploy the previous ETL version.

## 8. How this gets verified

**Unit, without StarRocks**, on facts built in Python:
- re-sequenced parent: a newer completed experiment for the father, aligned with a gVCF, makes an annotated trio `run` again; not aligned yet, the trio stays `done` until its gVCF lands (as today);
- experiment shared between two cases: annotating it on case 1 leaves case 2 `run`;
- two gVCFs on the current alignment: `excluded` / `ambiguous_gvcf`, with the member row; two alignments with one gVCF each: the current one is used, `run`;
- `done`, `no_trigger_document`, and each existing reason of each pipeline;
- the write filter: `done` only after a `run` at `discover`, `no_trigger_document` only for `submitted` cases.

**Equivalence, in each PR:** on the seeds, `decide_<pipeline>` over the fact query gives the same `run` units as the old pending query and selection. The old query is deleted in the same PR, after this test passes.

**Integration:** the fact query on the seeds, and `EXPLAIN` in `test_queries.py`.

**QA:** one daily run before and after each PR: the same units launched, the same status changes.

## 9. Out of scope

- The import's scenarios, moved to RAD-291: the slivar proband rule, a deleted task, a normal shared between a germline and a somatic case (RAD-266). They live in the import's staging view, not in the case selection.
- The import ledger and input fingerprint (RAD-291), `partition_assignment` (RAD-292), the task output rules (RAD-289).
- `done` for units completed by a child run by hand (§4).
- A per-case "current state" view for operators, and a `who acts` column on the reasons.
- The supersession rules of the RAD-287 views.
- The layout of `radiant/tasks/nextflow/`: germline SNV sits at its root and the other pipelines import from it (`CnvMember` extends the SNV row). Moving it to its own package and extracting the shared parts is a separate refactor, before PR 2.

## 10. Decisions

- The import's three scenarios move to RAD-291 (§9).
- `done` is a decision deduced from the facts, never a status written by anyone. Only the scheduler writes the log, as in RAD-288.
- `done` is logged at `after` only, for the units decided `run` at `discover` of the same run.
- A case with no trigger document is `skipped`, reason `no_trigger_document`. Logged for `submitted` cases only: proposed, to confirm in review.
- Two PRs: germline SNV first, to set the pattern; then germline CNV, QC and somatic SNV together.

## 11. Delivery

Two PRs in `radiant-portal-pipeline`, on top of RAD-288 (#353):
1. `feat/rad-290-selection-facts` (#354): germline SNV, with `DONE`, `no_trigger_document`, the write filter and `read_facts`. It sets the pattern.
2. `feat/rad-290-facts-cnv-qc-somatic`: germline CNV, QC (germline and somatic) and somatic SNV, on top of the `tasks/nextflow` refactor (#357). The case-based pipelines share `decide_cases` in `selection.py`.

Each carries its fact queries and models, the selections, the unit scenarios, the equivalence check on the seeds, and the docs.
