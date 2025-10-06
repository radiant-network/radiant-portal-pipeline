# Deploying Radiant ETL on MWAA and ECS

## Files 

- `requirements-mwaa.txt`: Lists the dependencies required for MWAA.
- `startup.sh`: Startup script used by MWAA to setup the environment. (This should be stored in S3 and referenced in MWAA configuration).

## Deployment Checklist

- [ ] Upload the `requirements-mwaa.txt` file to S3.
- [ ] Upload the `startup.sh` script to S3.

## Uploading the DAGs to S3

```
aws s3 cp --recursive radiant s3://radiant-tst-airflow-qa/dags/radiant --exclude="__pycache__/*"
```