# Deploying Radiant ETL on MWAA and ECS

## Files 

- `Dockerfile-mwaa-deps-builder`: Builds a Docker image to compile and package Radiant dependencies for MWAA.
- `requirements-deps.txt`: Lists the dependencies required for building the MWAA dependencies package.
- `requirements-mwaa.txt`: Lists the dependencies required for MWAA.
- `startup.sh`: Startup script used by MWAA to setup the environment. (This should be stored in S3 and referenced in MWAA configuration).

## Deployment Checklist

- [ ] Build the MWAA dependencies package using the provided Dockerfile.
- [ ] Upload the generated `plugins.zip` file to S3.
- [ ] Upload the `requirements-mwaa.txt` file to S3.
- [ ] Upload the `startup.sh` script to S3.
- [ ] Build and push the radiant operator image (see command below).


## ECS specific Airflow variables

These variables are necessary to configure the Radiant operator to run tasks on ECS. 
The ECS task definition doesn't allow configuration of those parameters in its configuration. They need to be passed when launching a task.

- `AWS_ECS_CLUSTER`: Name of the ECS cluster where Radiant tasks will run.
- `AWS_ECS_SUBNETS`: Comma-separated list of subnet IDs for ECS tasks.
- `AWS_ECS_SECURITY_GROUPS`: Comma-separated list of security group IDs used by ECS tasks.
- `AWS_ECS_S3_WORKSPACE`: S3 bucket path for Radiant workspace. 

## Building and Pushing the Radiant Operator Image

To build the radiant operator image, run:

```
docker build --platform linux/amd64 -t <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/radiant/radiant-vcf-operator:latest -f Dockerfile.radiant.operator .
```

To push the radiant operator image to ECR, use the following commands (replace `<aws_account_id>` with your AWS account ID):

```
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com
docker push <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/radiant/radiant-vcf-operator:latest
```

## Building the MWAA Dependencies

To build the Docker image for MWAA dependencies, run:

```
docker build -f Dockerfile-mwaa-deps-builder -t radiant-mwaa-deps .
```

Extract the dependencies package from the container:

```
docker run --rm -v $(pwd):/mwaa radiant-mwaa-deps cp /home/airflow/.venv/radiant/plugins.zip /mwaa/
``` 

## Uploading the DAGs to S3

```
aws s3 cp --recursive radiant s3://radiant-tst-airflow-qa/dags/radiant --exclude="__pycache__/*" --exclude="*.pyc"
```