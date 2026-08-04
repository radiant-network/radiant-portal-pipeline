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
| `requirements-airflow.txt`  | Airflow dependencies needed for running Kubernetes-based dags. Airflow version `2.10.5`. |

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