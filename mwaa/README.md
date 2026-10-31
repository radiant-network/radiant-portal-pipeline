# Deploying Radiant ETL on MWAA and ECS

## Deployment checklist

Radiant runs on one MWAA environment: Airflow 3.3.1 (Python 3.12).

- [ ] Run terraform code in `radiant-portal-deployment/deployment/terraform/airflow/` to provision MWAA environment and other AWS resources (prerequisite)
- [ ] Build the MWAA dependencies package using the provided Dockerfile (step 1)
- [ ] Upload artifacts to S3 (step 2)
- [ ] Upload dags to S3 (step 3)
- [ ] Build and push the radiant operator image (step 4).
- [ ] Build and push the dbt data-quality image (step 4b).
- [ ] Run terraform apply (step 5)


## Prerequisites

- Terraform has been applied for the target environment. This creates the bucket, the MWAA environment, the ECS task definition, the IAM roles, the Airflow Variables and Connections (as Secrets Manager secrets).
- You have AWS CLI access with permission to put objects in the MWAA bucket and to push to the Radiant ECR repository.
- Docker is installed locally for building images.


## Files

The airflow3 folder contains the files used to build the artifacts required by the MWAA environment.

- `Dockerfile-mwaa-deps-builder`: Builds a Docker image that compiles and packages Radiant dependencies for MWAA.
- `requirements-deps.txt`: Lists the dependencies required for building the MWAA dependencies package (used at build time by the Dockerfile). Pins `apache-airflow` to the environment's version.
- `requirements-mwaa.txt`: Used at runtime by MWAA to install dependencies (contains options to reuse pre-built wheels in the plugins zip).
- `constraints-3.12.txt`: Constraints file from the Airflow project (`constraints-3.3.1`); pins all transitive dependencies to versions compatible with the MWAA Airflow image.

To change the Airflow version, update together: `constraints-3.12.txt` (from `https://raw.githubusercontent.com/apache/airflow/constraints-<version>/constraints-3.12.txt`), the `apache-airflow` pin in `requirements-deps.txt`, the provider pins in both requirements files, and `airflow_v3_version` in `radiant-portal-deployment`.

> `startup.sh` is **not** stored here — Terraform generates it inline and uploads it to S3. See "What terraform handles for you" below.


## Step 1 - Building the MWAA Dependencies

Build the Docker image that produces the wheel bundle, then extract it.

```sh
AIRFLOW_VERSION=$(sed -n 's/^apache-airflow==//p' airflow3/requirements-deps.txt)
docker build -f airflow3/Dockerfile-mwaa-deps-builder -t radiant-mwaa-deps-af3 ..
docker run --rm -v $(pwd):/mwaa radiant-mwaa-deps-af3 \
  cp /home/airflow/.venv/radiant/plugins-airflow-${AIRFLOW_VERSION}.zip /mwaa/
```

You should now have `plugins-airflow-<X.Y.Z>.zip` in the current directory (ex: `plugins-airflow-3.3.1.zip`).
The version is the `apache-airflow` pin in `requirements-deps.txt`. The zip also carries it in an
`AIRFLOW_VERSION` file. The build fails if the pin is missing.

> The image is built on Amazon Linux 2023 so the wheels it produces are binary-compatible with the MWAA workers.


## Step 2 - Upload artifacts to S3

The environment expects its plugins bundle and requirements file at specific S3 paths.

For the QA environment (bucket: `radiant-tst-airflow-qa`):

```sh
BUCKET=radiant-tst-airflow-qa
```

The S3 keys are fixed by terraform, so the Airflow version goes in the object metadata
(`x-amz-meta-airflow-version`). Check that it matches `airflow_v3_version` before you apply.

```sh
aws s3 cp plugins-airflow-${AIRFLOW_VERSION}.zip s3://$BUCKET/plugins/plugins-mwaa3.zip \
  --metadata airflow-version=${AIRFLOW_VERSION}
aws s3 cp airflow3/requirements-mwaa.txt s3://$BUCKET/requirements-mwaa3.txt \
  --metadata airflow-version=${AIRFLOW_VERSION}

# Read it back (also works on an older object version with --version-id):
aws s3api head-object --bucket $BUCKET --key plugins/plugins-mwaa3.zip --query Metadata
```


## Step 3 - Upload the DAGs to S3

Run this from the repo root. MWAA picks up DAGs from this path automatically; no restart needed.

```sh
aws s3 sync radiant s3://radiant-tst-airflow-qa/dags/radiant --delete --exclude="*__pycache__*" --exclude="*.pyc"
```

## Step 4 — Build and push the Radiant operator image

This is the image referenced by the ECS task definition (terraform reads the tag from `var.operator_version`).

To build the radiant operator image, run:
```
docker build --platform linux/amd64 -t <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/radiant/radiant-vcf-operator:latest -f Dockerfile.radiant.operator .
```

To push the radiant operator image to ECR, use the following commands (replace `<aws_account_id>` with your AWS account ID):

```
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com
docker push <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/radiant/radiant-vcf-operator:latest
```

## Step 4b — Build and push the dbt data-quality image

This is the image referenced by the `radiant-dbt` ECS task definition (used by the
`data-integrity-starrocks` DAG). Build and push it once. Terraform reads the tag from the
dbt image version variable.

To build the dbt image, run:
```
docker build --platform linux/amd64 -t <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/radiant/radiant-dbt:latest -f Dockerfile.radiant.dbt .
```

To push it to ECR (replace `<aws_account_id>` with your AWS account ID):
```
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com
docker push <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/radiant/radiant-dbt:latest
```

## Step 5 - Run terraform apply

Only needed if you uploaded new plugins / requirements files (steps 1 and 2) and want MWAA to
pick them up. Terraform pins the environment to a specific S3 object **version**, so you set the
new version IDs in the target environment's tfvars through a PR.

Get the version ID of each uploaded artifact:

```sh
BUCKET=radiant-tst-airflow-qa
aws s3api head-object --bucket $BUCKET --key plugins/plugins-mwaa3.zip --query VersionId --output text  # plugins_v3_s3_object_version
aws s3api head-object --bucket $BUCKET --key requirements-mwaa3.txt    --query VersionId --output text  # requirements_v3_s3_object_version
```

Open a PR setting the matching variable(s) in the target environment's tfvars (QA:
[`.../qa/variables.tfvars`](https://github.com/radiant-network/radiant-portal-deployment/blob/main/deployment/terraform/airflow/radiant-tst/us-east-1/qa/variables.tfvars)):

```hcl
plugins_v3_s3_object_version="3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY"
```

Merging the PR creates a GitHub issue — follow its instructions to approve the apply. Terraform
updates the environment if its pinned version changed, and AWS restarts it.

> Leaving a variable empty falls back to the latest uploaded version. We don't recommend this
> unless for some reason there are no versions available (ex: clean-state bootstrap)


## What terraform handles for you (no manual action needed)

Provisioned automatically by
`radiant-portal-deployment/deployment/terraform/airflow/`:
- The S3 bucket (`<organization>-airflow-<environment>`) and its `dags/`,
  `plugins/`, `startup/` folder objects (all at the root).
- The `startup.sh` script — generated inline in `s3_bucket.tf` and uploaded by
  terraform itself to `startup/startup.sh`. More details about this script below.
- The MWAA environment (~25 min to provision).
- The ECS task definition (`airflow_ecs_operator_task`).
- All IAM roles and policies for MWAA, the ECS task, and the ECS
  execution role.
- The Airflow Variables, exposed via Secrets Manager under
  `airflow/variables/`:
  - `AWS_ECS_CLUSTER`
  - `AWS_ECS_SUBNETS`
  - `AWS_ECS_SECURITY_GROUPS`
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
| `RADIANT_ENVIRONMENT`                   | Deployment environment (ex: `qa`). Names the Radiant operator container targeted by the ECS overrides: `radiant-operator-<environment>-etl-container`. |
| `RADIANT_ICEBERG_NAMESPACE`             | Icerberg namespace for Radiant (ex: `radiant_qa`). |
| `RADIANT_IGV_TRACKS_S3_PREFIX`          | Default `igv_tracks_prefix` of `radiant-import-igv-tracks` (RAD-30): the S3 prefix the public IGV tracks and their `.tbi` are written under (ex: `s3://<tracks bucket>/igv/`). The task role must write there, the portal API role read it. No default: unset, `radiant-import-igv-tracks` fails on its first task (`list_igv_tracks`), and `radiant-import-open-data` with `import_igv_tracks: true` fails on `igv_tracks.trigger_import_igv_tracks`, after its tables are loaded. |
| `RADIANT_LOCK_S3_BUCKET`                | Bucket backing the `import_part` S3 mutex (design/SJRA-1811-opendatalake-integration.md §4). Must be set to the Airflow DAGs bucket (`<organization>-airflow-<environment>`, provisioned by terraform) -- NOT the data-lake bucket. Defaults to `warehouse`, the local/CI MinIO bucket, which does not exist in AWS. |
| `RADIANT_OPEN_DATA_CATALOG`             | StarRocks external catalog pointing at the OpenDataLake Iceberg warehouse (design/SJRA-1811-opendatalake-integration.md). Defaults to `opendatalake_catalog`. |
| `RADIANT_OPEN_DATA_DATABASE`            | Iceberg database inside that catalog. Per-environment (ex: `opendatalake_qa`), so every deployment must set it -- the default, `reference`, is the name OpenDataLake uses locally. |
| `RADIANT_OPEN_DATA_REF`                 | Iceberg ref every open-data read is pinned to. Defaults to `latest`, the tag OpenDataLake moves onto each publish; set it to a `dataset_version` to pin one release. It may not be empty: OpenDataLake keeps `main` permanently empty, so an unpinned read would return no rows on every source at once and the `INSERT OVERWRITE` downstream would empty the StarRocks target. An empty value is rejected when the mapping is built. |
| `RADIANT_OPEN_DATA_USE_LEGACY_TABLES`   | Sources held back on their pre-contract Radiant Iceberg tables instead of read from OpenDataLake. **Defaults to `*` -- every source held back**, so deploying this wheel into an environment that is not configured for OpenDataLake reads exactly what it read before; the cutover is an explicit config change, never a side effect of a deploy. Set it to the empty string for the target state (all 17 contract tables from OpenDataLake), or to a comma-separated list to migrate part way -- for a partial environment, or one whose publish is not ready. Accepts either name a source answers to (`mondo` or `mondo_term`). An unknown name is rejected rather than silently ignored, and `*` is honoured only as the whole value. |
| `RADIANT_S3_WORKSPACE`                  | Bucket for the temporary files Airflow hands to the Radiant tasks when the payload is too large to pass inline (ex: the merged partitions committed by `import_snv_vcf`). Set it to the data-lake bucket (`var.radiant_datalake_bucket`). Replaces the former `AWS_ECS_S3_WORKSPACE` Airflow Variable (SJRA-2006). Defaults to `warehouse`, the local/CI MinIO bucket. |
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


## Reference Airflow Connections

Connections are configured through the Secrets Manager, under the `airflow/connections` prefix.

The following connection secrets are expected to exist (as JSON or a connection string):
  - StarRocks connection (MySQL): `airflow/connections/starrocks_conn`
