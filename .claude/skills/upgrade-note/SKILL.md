---
name: upgrade-note
description: >-
  Decide whether the changes of the current branch need an operator action at deploy time, and if so write the
  upgrade note(s) in upgrade-notes/ (RAD-186 format), validate them with the PR check rules and preview them as the
  release PR will show them. Use it when the user asks for an upgrade note, an upgrade guide, deploy or migration
  steps for a branch or PR, "does this PR need a note", "what must ops do to deploy this", or to backfill the notes
  of the commits since a release tag. Also use it before opening a PR that changes a migration, an init SQL file,
  an Iceberg schema, an open-data insert, an env var, an Airflow Variable/connection/Param or a Nextflow pin.
---

# Upgrade note

The guide between two releases (`make upgrade-guide FROM=<vA> TO=<vB>`, the release PR description, the
"Upgrade guide" section of each GitHub release) is built only from the YAML files in `upgrade-notes/`. A step that no
note holds is missing for the operator. Your job: find every operator action the branch needs, prove each one from
the code, and write it so an operator can run it from the guide alone.

Read first: `upgrade-notes/README.md` (format, rules) and README "Upgrade guide". Model: `scripts/upgrade_guide/notes.py`.
PR check rules: `scripts/upgrade_guide/check.py`.

**Never claim an action from memory or from a ticket.** Every action, precondition and check must come from the diff,
a migration file header, a DAG doc (`radiant/dags/docs/*.md`) or the code. Cite the file in `refs`.

## 1. Scope

- Default: the branch against main, committed and not: `git diff --name-status main...HEAD` plus `git status --short`.
- Backfill of a release range (user asks "since the last tag"): `git log --format='%h %s' <tag>..HEAD`, one note per
  ticket. Those notes all land in the commit of the current PR, so they go to the next release.
- The ticket: from the branch name (`feat/rad-186` → `RAD-186`) or the commit subjects. Ask if unclear.

## 2. Find the actions

Go through the diff with this list. For each hit, read the file, not only the diff line.

| Signal in the diff | Usual action | Where to verify |
|---|---|---|
| New or changed file under `radiant/dags/sql/**/migrations/` | Run it; `when` and WHERE from its header ("HARD PREREQUISITE … BEFORE the new DAGs" → `before`) | The WHERE / WHEN / post-check comments of the file |
| New or changed `radiant/dags/sql/*/init/*.sql`, new table in `init_starrocks_tables.py` | Run `radiant-init-starrocks-base-tables` (with the `udf_release_version` in use). A changed view: drop it first, it is `CREATE VIEW IF NOT EXISTS` | `init_starrocks_tables.py` |
| Changed column list of a table that positional inserts or `SELECT *` copies write | `ALTER TABLE … ADD COLUMN` last, in every `<tenant>_tenant` database (per-tenant tables: `STARROCKS_RADIANT_PER_TENANT_MAPPING`), before the deploy, imports paused | `radiant/tasks/data/radiant_tables.py` |
| `+`/`-` on a `NestedField` of an Iceberg schema (`radiant/tasks/vcf/**`) | Recreate that one table: `python /opt/radiant/init_iceberg_table.py --table_name <table>` in the radiant-operator image of the release. **Never** the `radiant-init-iceberg-tables` DAG: one serial chain whose first tasks drop the SNV tables | `scripts/ecs/init_iceberg_table.py`, `radiant/tasks/iceberg/initialization.py`. A refactor with no `NestedField` change needs nothing |
| Edited open-data insert (`sql/open_data/*_insert.sql`) or new open-data table | `radiant-import-open-data` with `force_import = true`: its gate compares snapshots and does not see SQL or new tables | Param in `import_open_data.py` |
| Edited re-annotation or CNV/SNV annotation statement whose existing rows must change | `radiant-reannotate-open-data` with `force_reannotation = true` (holds the import lock); frequency statements: `recompute_frequencies` | Params in `reannotate_open_data.py` |
| New `os.environ` / `os.getenv`, Airflow `Variable.get`, `conn_id`, DAG `Param` with no safe default | Set it in each deployment (MWAA, QLIN Helm) | grep `^\+` lines of the diff |
| `Dockerfile.nextflow.launcher` pins, a file the pipeline reads from a mount (e.g. `NEXTFLOW_QC_PLACEHOLDER_VCF`) | Put the file in place, or check the pin with bioinfo | The DAG doc of the cases DAG |
| Backfill that marks cases pending (staging rows re-marked) | Pause `radiant-case-scheduler` first: on its next run it emails the lab of every pending case past `processing` | `radiant/tasks/nextflow/case_status.py` |
| New column the portal reads | Deploy order: ETL migration, then portal backend, then the next `radiant-import` | The radiant-hub feature doc, "Merge and rollout order" |
| PR title with `!` | A note is required, whatever the change | `check.py` |

No note for: image or dependency bumps only, CI, tests, docs, refactors with no schema/SQL/config change.
If nothing applies, write no file. Tell the user to put "No upgrade action" in the PR template's Upgrade section.

**Migration headers go stale.** A later commit can make a step useless or wrong. Check each header step against the
current code before you copy it. Seen in this repo: RAD-206 said "recreate somatic_cnv_occurrence" after #348 had
kept its schema, and said "run the init-iceberg-tables DAG", which RAD-256 forbids. The note says which header step
is out of date, and why.

**Shared steps run once.** A view drop + init run, a manual `radiant-import`, a pause/unpause: put the step in one
note, and in the others say "done by <ticket>'s step" in a precondition.

## 3. Write the note

`upgrade-notes/<TICKET>.yaml` (second PR of a ticket: `<TICKET>-2.yaml`):

```yaml
actions:
  - when: before            # before | after the deploy
    do: >-
      Add the slivar columns to the germline CNV occurrence table. Run the ALTER of the migration file in every
      <tenant>_tenant database (the base database for a single-tenant deployment), until FINISHED. Against an
      unmigrated table, every CNV import fails. New deployments must not run it.
    preconditions:          # optional
      - radiant-case-scheduler and radiant-reannotate-open-data are paused, and no import is running.
    refs:                   # optional: file paths or DAG ids, never a copy of the SQL
      - radiant/dags/sql/radiant/migrations/RAD-206_germline_cnv_occurrence_add_inheritance.sql
    check: SHOW ALTER TABLE COLUMN gives State = FINISHED, and DESC lists the fourteen new columns last.
```

Writing rules (the guide renders each action as a numbered subsection):

- **First sentence of `do` = the step title** (a heading). Each next sentence is one item of the step's "Change"
  list: one fact or order per sentence. A short order, under ~70 characters. Details, reasons and
  warnings ("Against an unmigrated table …", "Do not …") go in the next sentences.
- `check`: something the operator can run and read: a query and its expected result, a DAG run state, a file.
  **Self-contained:** the guide shows all checks of a phase in one table, away from the step text. Name the table,
  the database and the file ("Check 0 of the RAD-266 migration file", "radiant-init-starrocks-base-tables
  succeeds"), never "the migration file", "the run", "that path" or "step 2" (it clashes with the guide's numbers).
- `refs`: paths from the repo root. A path of this repo (a `/`, no space) becomes a link to the file at the
  release tag; a DAG id or a file of another repo ("radiant-hub docs/features/…") stays text.
- `preconditions`: real conditions only. The guide pools the preconditions of a phase in one "Before you start"
  block, each once, with the steps it applies to. Do not restate the step order ("X has run", "the steps above are
  done"): the numbers already give it. Write a shared condition with the exact same text in every note so it is
  listed once; the usual one is "No import is running (radiant-import, radiant-import-part,
  radiant-reannotate-open-data)."
- DAG ids as deployed: `radiant-<name>` (`NAMESPACE` prefix): `radiant-import`, `radiant-import-part`,
  `radiant-case-scheduler`, `radiant-import-open-data`, `radiant-reannotate-open-data`,
  `radiant-init-starrocks-base-tables`, `radiant-toolbox`.
- Public text: placeholders `<shared_db>`, `<tenant>_tenant`, `s3://<bucket>/…`. No IP, no host outside the
  allowlist of `check.py`, no 12-digit account, no real bucket, no patient data.
- YAML: use `>-` for any text with `: ` inside (a plain scalar `foo (X): bar` becomes a mapping and fails the model).
  No line that is exactly `---`.
- Order: within a release, notes follow the merge order of their PRs. Notes added by **one** PR share one squash
  commit and are sorted by file name. When a backfill puts several notes in one PR, put the steps that must run last
  (final import, unpause, forced re-annotation) in the last file, naming the tickets they serve.

## 4. Validate and preview

The CLI reads committed notes; validate the working tree with the same code the PR check runs:

```bash
source .venv/bin/activate && python - <<'EOF'
from pathlib import Path
from scripts.upgrade_guide.check import ChangedFile, check_pr
from scripts.upgrade_guide.guide import render_preview
texts = {str(p): p.read_text() for p in sorted(Path("upgrade-notes").glob("*.yaml"))}  # or only this PR's notes
print("errors:", check_pr("feat: TICKET title", "", [ChangedFile("A", p) for p in texts], texts))
print(render_preview(texts, pr="0"))
EOF
```

`errors: []` is required. Read the preview as the operator: do the step titles alone tell the order? Does each step
say where (base or every tenant database), when, and how to check it?

After a commit, the full guide: `make upgrade-guide FROM=<last tag> TO=HEAD`.

## 5. Report

Give the user, in short:

- The actions, as the numbered titles of the preview, and the evidence file of each.
- The header steps found out of date, and the shared steps merged.
- What you could not verify in this repo (a portal release, a file on a mount, an environment's settings).
- PR title: a PR that adds a note must be `feat`, `fix`, `perf` or `revert` (or have `!`): other types open no
  release and the `upgrade-note` check fails. Fill the PR template's "Upgrade" section.
