# Deploying Radiant ETL on MWAA and ECS

## Files 

- `Dockerfile-mwaa-deps-builder`: Builds a Docker image to compile and package Radiant dependencies for MWAA.
- `requirements-mwaa.txt`: Lists the dependencies required for MWAA.
- `startup.sh`: Startup script used by MWAA to setup the environment. (This should be stored in S3 and referenced in MWAA configuration).

## Deployment Checklist

- [ ] Build the MWA dependencies and upload the `plugins.zip` file in S3.
- [ ] Upload the `startup.sh` script to S3.

## Building the MWAA Dependencies

To build the Docker image for MWAA dependencies, run:

```
docker build -f Dockerfile-mwaa-deps-builder -t radiant-mwaa-deps .
```

Extract the dependencies package from the container:

```
docker run --rm -v $(pwd):/mwaa radiant-mwaa-deps cp /home/airflow/.venv/radiant/plugins.zip /mwaa/
``` 
