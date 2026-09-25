"""Import the COSMIC Mutation Census into StarRocks.

The census encodes indels without the VCF anchor base (an insertion has an empty reference allele, a
deletion an empty alternate), so its coordinates never match a VCF-derived locus as published. A
normalize task (radiant-operator image, Kubernetes or ECS) rebuilds each row as a VCF record against the
GRCh38 reference, left-aligns it with bcftools and writes a keyed TSV back to S3, which the StarRocks load
then reads. See `radiant/tasks/open_data/cosmic_mutation_set.py` and docs/import_cosmic_mutation_set.md.
"""

import logging
import os

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param

from radiant.dags import DEFAULT_ARGS, ICEBERG_NAMESPACE, IS_AWS, NAMESPACE, ECSEnv, load_docs_md
from radiant.tasks.starrocks.operator import RadiantStarrocksLoadOperator, RadiantStarRocksOperator, SubmitTaskOptions

if IS_AWS:
    from radiant.dags.operators import ecs as operators
else:
    from radiant.dags.operators import k8s as operators

LOGGER = logging.getLogger(__name__)

REFERENCE_FASTA_ENV = "RADIANT_REFERENCE_FASTA_S3_URI"
NORMALIZED_SUFFIX = ".normalized.tsv.gz"


def normalized_filepath_for(input_filepath: str, override: str | None = None) -> str:
    """Where the normalize task writes: the override if given, else next to the input with the
    ``.tsv``/``.tsv.gz`` extension replaced by ``.normalized.tsv.gz``."""
    if override:
        return override
    stem = input_filepath
    for ext in (".gz", ".tsv"):
        stem = stem.removesuffix(ext)
    return f"{stem}{NORMALIZED_SUFFIX}"


dag_params = {
    "cosmic_mutation_set_filepath": Param(
        type="string",
        title="COSMIC Mutation Census file",
        description=(
            "S3 path of the COSMIC Mutation Census export (cmc_export.tsv.gz), unchanged. The staging table "
            "is truncated first, so this is a full replace."
        ),
    ),
    "reference_fasta_filepath": Param(
        default=os.getenv(REFERENCE_FASTA_ENV, ""),
        type="string",
        title="GRCh38 reference FASTA",
        description=(
            f"S3 path of the reference FASTA bcftools left-aligns against; its .fai must sit next to it. "
            f"Defaults to the {REFERENCE_FASTA_ENV} environment variable."
        ),
    ),
    "normalized_filepath": Param(
        default=None,
        type=["string", "null"],
        title="Normalized file (output)",
        description=(
            "S3 path the normalize task writes and the load reads. Leave empty to write next to the input "
            "with a .normalized.tsv.gz extension."
        ),
    ),
}

with DAG(
    dag_id=f"{NAMESPACE}-import-cosmic-mutation-set",
    dag_display_name="Radiant - Import COSMIC Mutation Set",
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params=dag_params,
    render_template_as_native_obj=True,
    tags=["radiant", "starrocks", "open-data", "manual"],
    doc_md=load_docs_md("import_cosmic_mutation_set.md"),
) as dag:

    @task(task_id="resolve_normalized_filepath", task_display_name="[PyOp] Resolve Normalized Filepath")
    def resolve_normalized_filepath(params: dict | None = None) -> str:
        params = params or {}
        if not params.get("reference_fasta_filepath"):
            raise ValueError(f"reference_fasta_filepath is empty: set the param or {REFERENCE_FASTA_ENV}")
        return normalized_filepath_for(params["cosmic_mutation_set_filepath"], params.get("normalized_filepath"))

    normalized_filepath = resolve_normalized_filepath()

    if IS_AWS:
        normalize = operators.CosmicMutationSet.get_normalize(ecs_env=ECSEnv())
        normalized_filepath >> normalize
    else:
        # The Iceberg namespace is only part of the shared pod context; this task never touches Iceberg.
        normalize = operators.CosmicMutationSet.get_normalize(radiant_namespace=ICEBERG_NAMESPACE)(
            input_filepath="{{ params.cosmic_mutation_set_filepath }}",
            reference_fasta_filepath="{{ params.reference_fasta_filepath }}",
            output_filepath=normalized_filepath,
        )

    load_raw_cosmic_mutation_set = RadiantStarrocksLoadOperator(
        task_id="load_raw_cosmic_mutation_set",
        task_display_name="[StarRocks] Load Raw COSMIC Mutation Set",
        sql="./sql/open_data/cosmic_mutation_set_load.sql",
        table="{{ mapping.starrocks_raw_cosmic_mutation_set }}",
        truncate=True,
        load_label="load_raw_cosmic_mutation_set_{{ ts_nodash }}_{{ ti.try_number }}",
        # A list, like the other loads: pymysql renders it as `('s3://...')`, the parentheses `DATA INFILE`
        # requires -- a bare string would render without them and fail to parse.
        parameters={"tsv_filepath": ["{{ ti.xcom_pull(task_ids='resolve_normalized_filepath') }}"]},
    )

    insert_cosmic_mutation_set_hashes = RadiantStarRocksOperator(
        task_id="insert_cosmic_mutation_set_hashes",
        task_display_name="[StarRocks] COSMIC Mutation Set Insert Hashes",
        sql="./sql/open_data/cosmic_mutation_set_insert_hashes.sql",
        submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
    )

    insert_cosmic_mutation_set = RadiantStarRocksOperator(
        task_id="insert_cosmic_mutation_set",
        task_display_name="[StarRocks] COSMIC Mutation Set Insert Data",
        sql="./sql/open_data/cosmic_mutation_set_insert.sql",
        submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
    )

    normalize >> load_raw_cosmic_mutation_set >> insert_cosmic_mutation_set_hashes >> insert_cosmic_mutation_set
