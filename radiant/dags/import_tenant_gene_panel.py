"""Manual DAG: replace a tenant's gene panels with the panels of one TSV file on S3.

The file is attached unchanged to one portal `PUT /{tenant}/gene_panels`; the portal parses it,
checks it and replaces the tenant's uploaded gene panels in one transaction. See
`radiant/tasks/gene_panel/upload.py` and docs/import_tenant_gene_panel.md.
"""

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from radiant.dags import DEFAULT_ARGS, NAMESPACE, load_docs_md


@dag(
    dag_id=f"{NAMESPACE}-import-tenant-gene-panel",
    dag_display_name="Radiant - Import Tenant Gene Panels",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    # Each upload replaces the tenant's full set: two runs at once would race on which one wins.
    max_active_runs=1,
    tags=["radiant", "portal", "manual"],
    doc_md=load_docs_md("import_tenant_gene_panel.md"),
    params={
        "tenant": Param(
            type="string",
            # No space at all: a blank value would pass minLength and become an empty path segment.
            pattern="^\\S+$",
            title="Tenant",
            description="Tenant code, for example `radiant`. The panels are replaced in this tenant only.",
        ),
        "gene_panel_filepath": Param(
            type="string",
            pattern="^s3://[^/]+/.+[^/]$",
            title="Gene panel file",
            description=(
                "S3 URI of the .tsv file with **all** the tenant's gene panels. Each upload replaces the previous "
                "one: a panel missing from the file is removed."
            ),
        ),
        "strict": Param(
            False,
            type="boolean",
            title="Strict",
            description="Reject the whole file when a row matches no Ensembl gene, instead of skipping the row.",
        ),
    },
)
def import_tenant_gene_panel():
    @task(
        task_id="upload_gene_panels",
        task_display_name="[PyOp] Upload the gene panel file to the portal",
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def upload_gene_panels() -> dict:
        from airflow.operators.python import get_current_context

        from radiant.tasks.gene_panel.upload import upload_gene_panels

        params = get_current_context()["params"]
        return upload_gene_panels(
            tenant=str(params["tenant"]).strip(),
            filepath=str(params["gene_panel_filepath"]).strip(),
            strict=bool(params["strict"]),
        )

    upload_gene_panels()


import_tenant_gene_panel()
