# Upgrade notes

One YAML file per PR that needs an operator action, named after the ticket: `RAD-171.yaml`, or `RAD-171-2.yaml` when
a ticket has a second PR. The guide between two releases is built from these files and the `vX.Y.Z` tags
(README, "Upgrade guide"):

```sh
make upgrade-guide FROM=v1.2.0 TO=v1.4.1
```

A PR needs a note when the operator must do something to deploy it: a migration, a new environment variable, an Airflow
connection or variable, a DAG to run once, a re-import, a Nextflow pipeline revision. The `upgrade-note` check fails a
PR that changes a migration file, or has a breaking change, with no note.

```yaml
actions:
  - when: after                # before or after the deploy
    do: >-                     # the first sentence is the step title: keep it short
      Delete the stale staging rows. Run check 0, then section 2 of the migration file, in the base database.
    preconditions:             # optional
      - No radiant-import or import_part run is active.
    refs:                      # optional: the migration file or the DAG; never a copy of the SQL
      - radiant/dags/sql/radiant/migrations/RAD-171_staging_shared_sequencing.sql
    check: Query 0 of the migration file returns no row.
```

Rules:

- The first sentence of `do` is the title of the step in the guide. Make it a short order, and put the details, the
  reasons and the warnings in the next sentences. The guide lists each next sentence as one item.
- The guide pools the `preconditions` of a phase (before or after the deploy) in one "Before you start" block, and
  the `check` of each step in one table. So a precondition is a real condition, not the step order, and the same
  condition has the same text in every note ("No import is running (radiant-import, radiant-import-part,
  radiant-reannotate-open-data)."). A check names its file, table or DAG: it is read away from its step.
- A `refs` entry that is a path of this repo (it has a `/` and no space) is a link to the file at the release tag
  (at the commit, for the next release and the PR preview). Write the path from the repo root. A DAG id
  (`radiant-import`) or a file of another repo ("radiant-hub docs/features/…") stays plain text.
- The notes are public. No IP address, no host outside the allowlist of `scripts/upgrade_guide/check.py`, no account
  number, no real bucket name (`s3://<bucket>/...`, `arn:aws:s3:::<bucket>`), no patient data. Use the placeholders of
  the migration files (`<shared_db>`, `<tenant>_tenant`). The check does not find a host name written with no scheme
  (`db.example:9030`): the reviewer must catch it.
- Never delete a note: its release would lose its actions. To correct a note, edit it in a new PR; the guide always
  reads the text on `main`.
- Add a note only in a `feat`, `fix`, `perf` or `revert` PR, or one with `!`: the other types open no release, so the
  note would wait for the next one.
