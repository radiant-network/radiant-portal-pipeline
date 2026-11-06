# radiant-portal-pipeline

This repository contains the different files and scripts used to run the Radiant ETL pipeline.

## Development

### Useful `make` commands

- `make install`: Installs the Python dependencies using `pip` in your virtual environment.
- `make test`: Runs the code static checks and tests using `ruff` and `pytest`.
- `make format`: Formats the code using `ruff`

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

**To release:** check that the `release-gate` check of the release PR passes (re-run it first, see below), then merge
the release PR. The merge puts `CHANGELOG.md` and `.release-please-manifest.json` on `main`; release-please then tags
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
- Ruleset of `main`: `pr-title` and `release-gate` are required status checks.

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