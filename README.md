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

(Linux Only) Create Airflow directories and set Airflow UUID:

```shell
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
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

Here is an example for connecting to the default StarRocks DB provided in the docker-compose file:

- `Connection Id`: `starrocks_conn`
- `Connection Type`: `MySQL`
- `Host`: `radiant-starrocks-fe`
- `Schema`: `poc_starrocks`
- `Login`: `root`
- `Password`: (leave empty)
