# radiant-portal-pipeline

This repository contains the different files and scripts used to run the Radiant ETL pipeline.

## Development

### Useful `make` commands

- `make install`: Installs the Python dependencies using `pip` in your virtual environment.
- `make test`: Runs the code static checks and tests using `ruff` and `pytest`.
- `make format`: Formats the code using `ruff`

### Structure

- `dags` directory contains the DAGs used to run the pipeline.
- `dags/lib` directory contains the libraries used in the DAGs.
- `lib` contains Airflow-specific components to support ETL pipelines.
- `tasks` directory contains the python tasks used in the DAGs.
- `docker` contain specific airflow image with dependencies to run the pipeline.


### Airflow dev stack

Build the airflow docker image :

```
docker build . -f docker/airflow.Dockerfile -t radiant-network/airflow-cyvcf2:2.10.5
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
