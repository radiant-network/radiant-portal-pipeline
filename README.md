# radiant-portal-pipeline

This repository contains the different files and scripts used to run the Radiant ETL pipeline.

## Development

### Useful `make` commands

- `make install`: Installs the Python dependencies using `pip` in your virtual environment.
- `make test`: Runs the code static checks and tests using `ruff` and `pytest`.
- `make format`: Formats the code using `ruff`
- `make upgrade-guide FROM=<vA> TO=<vB>`: Prints the upgrade guide between two releases (see "Upgrade guide").

### Structure

- `radiant/dags` directory contains the DAGs used to run the pipeline.
- `radiant/dags/task` directory contains the libraries used in the DAGs.
- `scripts` directory contains several utility scripts.
- `tests` directory contains the tests.


### Airflow dev stack

Build the airflow docker image :

```
make build-docker
```

Deploy stack :

```
docker compose up 
```

Login to Airflow UI :

- URL : `http://localhost:8080`
- Username : `airflow`
- Password : `airflow`

Create Airflow connection to StarRocks (Airflow UI => Admin => Connections)

The following are examples and should be adjusted to your environment:

- `Connection Id`: `starrocks_conn`
- `Connection Type`: `MySQL`
- `Host`: `host.docker.internal`
- `Schema`: `poc_starrocks`
- `Login`: `root`
- `Password`: (leave empty)

#### Socket timeouts (recommended for deployed environments)

Add socket timeouts to the connection's `Extra` field:

```json
{"connect_timeout": 10, "read_timeout": 60, "write_timeout": 60}
```

`StarRocksTaskCompleteTrigger` polls long-running StarRocks tasks from the triggerer. It already bounds each
poll with `asyncio.wait_for`, so a dead socket can neither block the triggerer's event loop nor stall the
poll loop. But the worker thread behind a timed-out poll stays blocked in `recv()` until the socket itself
errors out, holding a slot in the default thread pool — only a driver-level `read_timeout` releases it. This
matters when the FE's IP changes or a pod is rescheduled, leaving half-open connections behind.

Verify the extras against the `apache-airflow-providers-mysql` version in your image (`6.5.2` for the
K8s/MWAA builds) before relying on them: the hook forwards only whitelisted extras to the driver, so an
unsupported key can break `get_conn()` outright. A `Test` on the connection in the Airflow UI is enough to
confirm.


## Artifacts

### Requirements files

| Requirements File Name      | Purpose                                                                                  |
|-----------------------------|------------------------------------------------------------------------------------------|
| `requirements.txt` | Main dependencies required to run the Radiant ETL pipeline.                              |
| `requirements-dev.txt`      | Development dependencies such as testing and linting tools.                              |
| `requirements-airflow.txt`  | Airflow dependencies needed for running Kubernetes-based dags. Airflow version `3.3.1`.  |

### Dockerfiles

| Dockerfile                     | Purpose                                                                                            |
|--------------------------------|----------------------------------------------------------------------------------------------------|
| `Dockerfile`                   | Main image used by Airflow's webserver, scheduler, etc... in Kubernetes-based deployments.         |
| `Dockerfile-radiant-operator`  | Used as the pod images for running tasks with Radiant dependencies (for both K8S/ECS deployments). |


### Unit Tests

To run unit and fast integration tests:

```sh
python -m venv .venv
source .venv/bin/activate
make install-dev
USE_DOCKER_FIXTURES=true make test
```

This will execute all tests that do not require the external sandbox environment, using local Docker fixtures.

To run full integration tests (slow or fast) with the sandbox, follow these steps:

1. **Start the sandbox environment**:  See the radiant-portal-sandbox repository for setup instructions

2. **Run the tests**: Use the same virtual environment as above but set set USE_DOCKER_FIXTURES=false`:
```sh
USE_DOCKER_FIXTURES=false make test-integration-slow # For slow integration tests
USE_DOCKER_FIXTURES=false make test-integration # For fast (non slow) integration tests
```

### MWAA Artifacts

Running the Radiant ETL pipeline using AWS MWAA and ECS requires additional setup. 

The `mwaa/` directory contains files and documentation specific to deploying the Radiant ETL pipeline on MWAA and ECS.

## Release

The ETL is released from its conventional commits by [release-please](https://github.com/googleapis/release-please)
(`.github/workflows/release.yml`). On each push to `main`, it opens or updates one release PR,
`chore(main): release <version>`: the version is computed from the commit types since the last `vX.Y.Z` tag, and the
PR body is the changelog. Nothing is released until a person merges that PR.

| Commit type (PR title) | Version bump |
|---|---|
| `feat!:`, `fix!:` … (any type with `!`), or a `BREAKING CHANGE:` footer | major |
| `feat:` | minor |
| `fix:`, `perf:`, `revert:` | patch |
| `docs:`, `style:`, `chore:`, `refactor:`, `test:`, `build:`, `ci:` | none: alone, they open no release PR and are not in the changelog |

PRs are squash-merged, so the PR title (checked by `pr-title.yml`) becomes the commit subject that release-please reads.

**To release:** read the upgrade guide at the end of the release PR description (see "Upgrade guide"), check that the `release-gate`
check of the release PR passes (re-run it first, see below), then merge the release PR. The merge puts `CHANGELOG.md` and `.release-please-manifest.json` on `main`; release-please then tags
that commit `vX.Y.Z` and creates the GitHub release with the changelog. The tag starts the three `build_and_push_*`
workflows, which push the semver images. Merging the release PR deploys nothing.

Setup (once, by an admin):

- The workflow uses a GitHub App token, because events created with the default `GITHUB_TOKEN` start no workflow: the
  release PR would run no check and the tag would build no image. It uses the org App `radiant-release-please`
  (org variable `RELEASE_PLEASE_APP_CLIENT_ID`, org secret `RELEASE_PLEASE_APP_PRIVATE_KEY`; radiant-hub
  `docs/ci/security-checks.md`), which must be installed on this repo with `Contents`, `Issues` and `Pull requests` set
  to read and write. With no write permission, the workflow fails: no release PR, no
  tag.
- Repo settings: squash merge only, with the PR title as the squash commit message.
- Ruleset of `main`: `pr-title`, `release-gate` and `upgrade-note` are required status checks.

Config: `release-please-config.json` (release type `simple`; there is no `version.txt`, so only the changelog and the
manifest change; `pyproject.toml` keeps its static version) and `.release-please-manifest.json` (the last released
version). The config bootstraps from the commit of `v1.1.32`, the last tag made by hand. Do not tag by hand any more: the
tag still builds the images, but release-please looks only for the release of the version in the manifest, so it does
not see the hand tag and can propose the same version again.

### Release gate

A release goes to prod, so the release PR (opened by release-please from the branch `release-please--branches--main`)
cannot merge while a known vulnerability is open. The `Release Gate` workflow (`.github/workflows/release-gate.yml`)
runs on that PR only: on every other PR its jobs are skipped and the check passes. Make its `release-gate` job a required
status check in the ruleset of `main`: it fails unless both checks pass:

| Check | Fails when | Where to look |
|---|---|---|
| `dependabot` | An open Dependabot alert of severity high or critical exists, in any ecosystem. | The job summary lists the alerts; Security → Dependabot. |
| `trivy`, one job per image (task operator, dbt, Nextflow launcher) | Trivy finds a HIGH or CRITICAL vulnerability that has a fix in the image built from the PR (`ignore-unfixed`), the entries of `.trivyignore.yaml` left out. Vulnerabilities only (`scanners: vuln`): the secret scanner stays with the Trivy workflow and the Security tab. | The job summary holds the Trivy table. |

To unblock the release, fix the vulnerability in a normal PR (bump the dependency or the base image): its merge updates the
release PR, and the gate runs again. To accept the risk instead:

- **Dependabot alert:** dismiss it in Security → Dependabot, with a reason. A dismissed alert is no longer open, so the
  gate no longer counts it. Then re-run the failed run of the gate (Actions → Release Gate → Re-run all jobs).
- **Trivy finding:** add an entry to `.trivyignore.yaml`, in a reviewed PR: the vulnerability `id`, the `paths` of the one
  file it applies to, a `statement` with the reason and the ticket, and an `expired_at` date that brings the finding back
  for review. The Trivy workflow reads the same file, so the finding leaves the Security tab too.

> **Trap:** "Dismiss alert" in Security → Code scanning does not unblock the gate. It closes the alert the Trivy
> workflow uploaded: the next scan does not raise it again, and the code scanning check of a feature PR passes. The
> gate reads no code scanning alert. It scans the image it builds and reads `.trivyignore.yaml`, nothing else, so the
> finding fails it again. Accept a Trivy finding through the ignore file, never through the Security tab. A dismissed
> Dependabot alert is different: both the gate and the Security tab read the same alert, so one dismissal is enough.

The result goes stale while the release PR waits: an alert published after the last run does not fail it. The person who
merges the release PR re-runs the workflow first. GitHub lets you re-run a workflow for 30 days after its first run: if the
last run is older, close and reopen the release PR, which starts a new run.

To test the gate without a release PR, open a temporary PR from a branch whose name starts with
`release-please--branches--main`: the gate runs on it. Delete the branch afterwards.

## Upgrade guide

A PR whose deploy needs an operator action adds one **upgrade note** in `upgrade-notes/`: a YAML file named after the
ticket (`RAD-171.yaml`, `RAD-171-2.yaml` for a second PR of the same ticket). The guide between two releases lists the
actions of the notes of every release in between, so an environment that skips releases sees all of them.

**When a PR needs a note:** a migration file (`radiant/dags/sql/**/migrations/`), a new environment variable, a new
Airflow connection or variable, a DAG to run once, a re-import, a Nextflow pipeline revision, or any breaking change.
Otherwise, write "No upgrade action" in the "Upgrade" section of the PR description.

**Format** (validated by `scripts/upgrade_guide/notes.py`): a list of actions, each with `when` (`before` or `after`
the deploy), `do`, optional `preconditions` and `refs` (the migration file or the DAG; the note does not copy the SQL,
the migration file keeps its WHERE / WHEN header), and `check` (how to see that it worked). The first sentence of `do`
is the title of the step in the guide: make it a short order ("Add the slivar columns to the germline CNV occurrence
table."), and put the details in the next sentences.

```yaml
actions:
  - when: after
    do: Delete the somatic rows of the germline tasks in the staging table.
    preconditions:
      - No radiant-import or import_part run is active.
    refs:
      - radiant/dags/sql/radiant/migrations/RAD-266_germline_tasks_out_of_somatic_cases.sql
    check: Query 0 of the migration file returns no row.
```

**The notes are public** (job summary, release PR, GitHub release): no IP address, no host outside the allowlist of
`scripts/upgrade_guide/check.py`, no 12-digit account number, no real bucket name (`s3://<bucket>/...`,
`arn:aws:s3:::<bucket>`), no patient data. Use the placeholders of the migration files (`<shared_db>`,
`<tenant>_tenant`). The check finds the host of a URL (`https://...`, `jdbc:mysql://...`), not a host name written
with no scheme (`db.example:9030`): the reviewer must catch that one.

**Breaking changes:** put `!` after the type in the PR title (`feat!: ...`). The squash commit message is the PR title
only, so a `BREAKING CHANGE:` footer in the PR description never reaches release-please. The check still reads it.

**PR check** (`.github/workflows/upgrade-note.yml`, job `upgrade-note`, rules in `scripts/upgrade_guide/check.py`). The
job runs the checker of the base branch, so a PR cannot relax the rules it is checked by: a change to the rules applies
from the next PR. It fails when:

- the PR adds or changes a migration file and adds or changes no note;
- the PR title has `!`, or its description a `BREAKING CHANGE:` footer, and the PR adds no note;
- a note does not match the format, or holds one of the public-data values above;
- a file in `upgrade-notes/` is not a note (a subfolder, a `.YAML` extension): neither the check nor the guide would
  read it;
- the PR deletes a note (to correct a note, edit it: the guide always reads the text on `main`);
- the PR adds a note and its type is not `feat`, `fix`, `perf` or `revert` (with no `!`): such a PR opens no release,
  so its note would wait for the next one.

The job summary of the check (Checks → `upgrade-note` → Summary) previews the notes the PR adds or changes, as the
guide will show them: the reviewer reads the actions as the operator will, not only the YAML.

**Release of a note:** the first `vX.Y.Z` tag that contains the commit that added the note file (the PR's squash
commit). The actions of a release are in merge order, grouped "Before the deploy" and "After the deploy". A release
with no note shows "No upgrade action".

**The guide between two versions:**

- From a clone with the tags (`git fetch --tags`), on `main`: `make upgrade-guide FROM=v1.2.0 TO=v1.4.1`. `TO` can be
  `main` for the notes not released yet. The note text is read from the committed `HEAD`. It fails, naming the version,
  when `FROM` is not older than `TO` or a version is not a tag. When `FROM` is older than the first release with the
  `upgrade-notes/` folder, it warns that the actions of the earlier releases are not listed: read their changelog and
  migration files.
- Without a clone: Actions → Upgrade Guide → Run workflow on `main` (`.github/workflows/upgrade-guide.yml`), with
  `from` and `to`. The guide is in the job summary.
- The Release workflow adds the guide from the last release to `main` at the end of the open release PR description,
  after the release-please footer, between the markers `<!-- upgrade-guide:start -->` and `<!-- upgrade-guide:end -->`.
  release-please rebuilds the description when the release changes, and the same job adds the guide back. On the
  merge, release-please reads the release notes only between the first and the last `---` line, so the guide never
  reaches the changelog.
- The Upgrade Guide Release workflow (`.github/workflows/upgrade-guide-release.yml`) adds an "Upgrade guide" section
  to each GitHub release when it is published. To add a missing section, or to refresh it after a note was corrected,
  run it on `main` with the `tag` input. A tag with no GitHub release gets no section; the command still covers its
  range.
