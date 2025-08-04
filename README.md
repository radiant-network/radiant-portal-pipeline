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

## Documentation

### Requirements files

| Requirements File Name     | Purpose                                                                            |
|----------------------------|------------------------------------------------------------------------------------|
| `requirements.txt`         | Main dependencies required to run the Radiant ETL pipeline.                        |
| `requirements-dev.txt`     | Development dependencies such as testing and linting tools.                        |
| `requirements-airflow.txt` | Dependencies needed for Kubernetes-based deployment. Airflow version `2.10.5`.     |
| `requirements-mwaa.txt`    | Dependencies needed for the MWAA (AWS)-based deployment. Airflow version `2.10.3`. |

### Dockerfiles

| Dockerfile                | Purpose                                                                            |
|---------------------------|------------------------------------------------------------------------------------|
| `Dockerfile`              | Main image used by webserver, scheduler, etc... in Kubernetes-based deployments.   |
| `Dockerfile-mwaa`         | Used as a dependencies builder to create an initialization package for MWAA.       |
| `Dockerfile-vcf-operator` | Used at the pod images for running VCF import task (for both K8S/ECS deployments). |
