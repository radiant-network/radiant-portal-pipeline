# radiant-portal-pipeline

This repository contains the different files and scripts used to run the Radiant ETL pipeline. 

## Overview

.

## Development

.

### Airflow dev stack

Create `.env` file :

```
cp .env.sample .env
```

Deploy stack :

```
docker-compose up
```

Login to Airflow UI :

- URL : `http://localhost:50080`
- Username : `airflow`
- Password : `airflow`

Create Airflow variables (Airflow UI => Admin => Variables) :

- environment : `qa`
- kubernetes_namespace : `cqgc-qa`
- kubernetes_context_default : `kubernetes-admin-cluster.qa.cqgc@cluster.qa.cqgc`
- kubernetes_context_etl : `kubernetes-admin-cluster.etl.cqgc@cluster.etl.cqgc`
- base_url (optional) : `http://localhost:50080`
- show_test_dags (optional) : `yes`

_For faster variable creation, upload the `variables.json` file in the Variables page._

Test one task :

```
docker-compose exec airflow-scheduler airflow tasks test <dag> <task> 2022-01-01
```