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


## Artifacts

### Requirements files

| Requirements File Name      | Purpose                                                                                  |
|-----------------------------|------------------------------------------------------------------------------------------|
| `requirements-operator.txt` | Main dependencies required to run the Radiant ETL pipeline.                              |
| `requirements-dev.txt`      | Development dependencies such as testing and linting tools.                              |
| `requirements-airflow.txt`  | Airflow dependencies needed for running Kubernetes-based dags. Airflow version `2.10.5`. |

### Dockerfiles

| Dockerfile                     | Purpose                                                                                            |
|--------------------------------|----------------------------------------------------------------------------------------------------|
| `Dockerfile`                   | Main image used by Airflow's webserver, scheduler, etc... in Kubernetes-based deployments.         |
| `Dockerfile-radiant-operator`  | Used as the pod images for running tasks with Radiant dependencies (for both K8S/ECS deployments). |

### MWAA Artifacts

Running the Radiant ETL pipeline using AWS MWAA and ECS requires additional setup. 

The `mwaa/` directory contains files and documentation specific to deploying the Radiant ETL pipeline on MWAA and ECS.