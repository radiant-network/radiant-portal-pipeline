# Deploying Radiant ETL on MWAA and ECS

## Deployment checklist

Radiant runs on two MWAA environments — Airflow 2 (Python 3.11) and Airflow 3 (Python 3.12). Steps 1 and 2 differ slightly between the two, so we provide environment-specific instructions for them. The other steps only need to be performed once and cover both environments.

- [ ] Run terraform code in `radiant-portal-deployment/deployment/terraform/airflow/` to provision MWAA environment and other AWS resources (prerequisite)
- [ ] Build the MWAA dependencies package(s) using the provided Dockerfile — once per target version (step 1)
- [ ] Upload artifacts to S3 (step 2)
- [ ] Upload dags to S3 (step 3)
- [ ] Build and push the radiant operator image (step 4).
- [ ] Run terraform apply (step 5)


## Prerequisites

- Terraform has been applied for the target environment. This creates the bucket, the MWAA environment, the ECS task definition, the IAM roles, the Airflow Variables and Connections (as Secrets Manager secrets).
- You have AWS CLI access with permission to put objects in the MWAA bucket and to push to the Radiant ECR repository.
- Docker is installed locally for building images.


## Files

The airflow2 and airflow3 folders contain a similar set of files, used to build the artifacts required by the Airflow 2 and Airflow 3 environments respectively.

- `Dockerfile-mwaa-deps-builder`: Builds a Docker image that compiles and packages Radiant dependencies for MWAA.
- `requirements-deps.txt`: Lists the dependencies required for building the MWAA dependencies package (used at build time by the Dockerfile).
- `requirements-mwaa.txt`: Used at runtime by MWAA to install dependencies (contains options to reuse pre-built wheels in `plugins.zip`).
- `constraints-3.11.txt` / `constraints-3.12.txt`: Constraints file from the Airflow project; pins all transitive dependencies to versions compatible with the MWAA Airflow image (3.11 for Airflow 2, 3.12 for Airflow 3).

> `startup.sh` is **not** stored here — Terraform generates it inline and uploads it to S3. See "What terraform handles for you" below.


## Step 1 - Building the MWAA Dependencies

Build the Docker image that produces the wheel bundle, then extract `plugins.zip`
under a version-specific local name.

**Airflow 2:**

```sh
docker build -f airflow2/Dockerfile-mwaa-deps-builder -t radiant-mwaa-deps-af2 airflow2
docker run --rm -v $(pwd):/mwaa radiant-mwaa-deps-af2 \
  cp /home/airflow/.venv/radiant/plugins.zip /mwaa/plugins-af2.zip
```

You should now have `plugins-af2.zip` in the current directory.

**Airflow 3:**

```sh
docker build -f airflow3/Dockerfile-mwaa-deps-builder -t radiant-mwaa-deps-af3 airflow3
docker run --rm -v $(pwd):/mwaa radiant-mwaa-deps-af3 \
  cp /home/airflow/.venv/radiant/plugins.zip /mwaa/plugins-af3.zip
```

You should now have `plugins-af3.zip` in the current directory.

> The image is built on Amazon Linux 2023 so the wheels it produces are binary-compatible with the MWAA workers.


## Step 2 - Upload artifacts to S3

Each environment expects its plugins bundle and requirements file at specific S3 paths.

For the QA environment (bucket: `radiant-tst-airflow-qa`):

```sh
BUCKET=radiant-tst-airflow-qa
```

**Airflow 2:**

```sh
aws s3 cp plugins-af2.zip s3://$BUCKET/plugins/plugins.zip
aws s3 cp airflow2/requirements-mwaa.txt s3://$BUCKET/requirements-mwaa.txt
```

**Airflow 3:**

```sh
aws s3 cp plugins-af3.zip s3://$BUCKET/plugins/plugins-mwaa3.zip
aws s3 cp airflow3/requirements-mwaa.txt s3://$BUCKET/requirements-mwaa3.txt
```


## Step 3 - Upload the DAGs to S3

Both environments share the same `dags/` folder, so upload it once — it
serves Airflow 2 and Airflow 3. Run this from the repo root. MWAA picks up DAGs
from this path automatically; no restart needed.

```sh
aws s3 sync radiant s3://radiant-tst-airflow-qa/dags/radiant --delete --exclude="*__pycache__*" --exclude="*.pyc"
```

> `sync --delete` removes from the bucket any file that no longer exists locally —
> without it, DAGs deleted from the repo would linger in S3 and keep being parsed
> by MWAA.

## Step 4 — Build and push the Radiant operator image

This is the image referenced by the ECS task definition (terraform reads the tag from `var.operator_version`). It is shared by both environments — build and push it once.

To build the radiant operator image, run:
```
docker build --platform linux/amd64 -t <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/radiant/radiant-vcf-operator:latest -f Dockerfile.radiant.operator .
```

To push the radiant operator image to ECR, use the following commands (replace `<aws_account_id>` with your AWS account ID):

```
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com
docker push <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/radiant/radiant-vcf-operator:latest
```

## Step 5 - Run terraform apply

Only needed if you uploaded new plugins / requirements files (steps 1 and 2) and want MWAA to
pick them up. Terraform pins each environment to a specific S3 object **version**, so you set the
new version IDs in the target environment's tfvars through a PR.

Get the version ID of each uploaded artifact:

```sh
BUCKET=radiant-tst-airflow-qa
aws s3api head-object --bucket $BUCKET --key plugins/plugins.zip       --query VersionId --output text  # plugins_s3_object_version
aws s3api head-object --bucket $BUCKET --key requirements-mwaa.txt     --query VersionId --output text  # requirements_s3_object_version
aws s3api head-object --bucket $BUCKET --key plugins/plugins-mwaa3.zip --query VersionId --output text  # plugins_v3_s3_object_version
aws s3api head-object --bucket $BUCKET --key requirements-mwaa3.txt    --query VersionId --output text  # requirements_v3_s3_object_version
```

Open a PR setting the matching variable(s) in the target environment's tfvars (QA:
[`.../qa/variables.tfvars`](https://github.com/radiant-network/radiant-portal-deployment/blob/main/deployment/terraform/airflow/radiant-tst/us-east-1/qa/variables.tfvars)):

```hcl
plugins_s3_object_version="3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY"
```

Merging the PR creates a GitHub issue — follow its instructions to approve the apply. Terraform
updates only the environment(s) whose pinned version changed, and AWS restarts them.

> Leaving a variable empty falls back to the latest uploaded version. We don't recommend this
> unless for some reason there are no versions available (ex: clean-state bootstrap)


## What terraform handles for you (no manual action needed)

Provisioned automatically by
`radiant-portal-deployment/deployment/terraform/airflow/`:
- The S3 bucket (`<organization>-airflow-<environment>`) and its `dags/`,
  `plugins/`, `startup/` folder objects (all at the root, shared by both
  environments).
- The `startup.sh` script — generated inline in `s3_bucket.tf` and uploaded by
  terraform itself to `startup/startup.sh`, shared by both environments. More
  details about this script below.
- Both MWAA environments (~25 min to provision each).
- The ECS task definition (`airflow_ecs_operator_task`).
- All IAM roles and policies for MWAA, the ECS task, and the ECS
  execution role.
- The Airflow Variables, exposed via Secrets Manager under
  `airflow/variables/`:
  - `AWS_ECS_CLUSTER`
  - `AWS_ECS_SUBNETS`
  - `AWS_ECS_SECURITY_GROUPS`
  - `AWS_ECS_S3_WORKSPACE`
   You do **not** configure these in the Airflow UI. Their values come
   from `variables.tfvars` for the target environment.

- The Airflow Connections, exposed via Secrets Manager under `airflow/connections/`.


## Reference: environment variables set by the startup script

Terraform generates `startup.sh`, which will be executed by the MWAA workers when they start.

This ensure that they have the right environment when they start. The current variables are:

| Variable                                | Description |
|-----------------------------------------|-------------|
| `IS_AWS`                                | Set to `true` to indicate running in AWS environment. |
| `PYICEBERG_CATALOG__DEFAULT__TYPE`      | Set to `glue` for PyIceberg catalog type. |
| `RADIANT_ICEBERG_NAMESPACE`             | Icerberg namespace for Radiant (ex: `radiant_qa`). |
| `RADIANT_TASK_OPERATOR_TASK_DEFINITION` | ECS task definition for Radiant operator. |
| `RADIANT_TASK_OPERATOR_LOG_GROUP`       | CloudWatch log group  for Radiant operator. |
| `RADIANT_TASK_OPERATOR_LOG_REGION`      | AWS region  for radiant operator logs | 
| `RADIANT_TASK_OPERATOR_LOG_PREFIX`      | Log prefix for radiant operator logs ex: (`ecs/radiant-operator-qa-etl-container`) |
| `STARROCKS_BROKER_USE_INSTANCE_PROFILE` | Set to `true` to use instance profile for StarRocks broker. |


## Reference: Airflow Variables

These are read by DAGs via `Variable.get(...)` and resolved through the Secrets Manager backend (configured in `aws_mwaa_environment.airflow_configuration_options`).

You don't set these by hand — Terraform reads them from your tfvars and writes them to Secrets Manager. For example, the variable `AWS_ECS_CLUSTER` is stored as the secret `airflow/variables/AWS_ECS_CLUSTER`.


| Variable                  | Comes from terraform variable            | Description |
|---------------------------|------------------------------------------| ----------- |
| `AWS_ECS_CLUSTER`         | `var.ecs_cluster_name`                   |  Name of the ECS cluster where Radiant tasks will run. |
| `AWS_ECS_SUBNETS`         | `var.private_subnets` (joined with `,`)  |  Comma-separated list of subnet IDs for ECS tasks. |
| `AWS_ECS_SECURITY_GROUPS` | `var.ecs_security_groups` (joined with `,`) | Comma-separated list of security group IDs used by ECS tasks. |
| `AWS_ECS_S3_WORKSPACE`    | `var.radiant_datalake_bucket`            |  S3 bucket path for Radiant workspace. |


## Reference Airflow Connections

Connections are configured through the Secrets Manager, under the `airflow/connections` prefix.

The following connection secrets are expected to exist (as JSON or a connection string):
  - StarRocks connection (MySQL): `airflow/connections/starrocks_conn`
