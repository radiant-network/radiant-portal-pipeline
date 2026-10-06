# Import Tenant Gene Panels

Replaces the gene panels of one tenant with the panels of one TSV file on S3. The DAG reads the
file and attaches it, unchanged, to one portal call: **PUT /{tenant}/gene_panels**. The portal
parses and checks the file, finds the Ensembl gene of each row, and replaces the tenant's uploaded
gene panels in one transaction. The new panels are available in the **tenant_gene_panel** variant
filter when the call returns.

**The file is the full list.** A panel of the tenant that is not in the file is removed. The panels
of the analysis catalog (prescriptions) are not changed. Sending the same file again gives the same
result, so a re-run or a retry is safe.

---

## Runbook

1. Get the TSV file from the clinical team. Check that it holds **all** the tenant's panels, not
   only the new ones.
2. Put it in the tenant's S3 prefix, for example
   s3://&lt;bucket&gt;/gene_panels/radiant/gene_panels.tsv.
3. Trigger this DAG with the parameters below.
4. Read the log of **upload_gene_panels**: one line with the panel and gene counts, and one warning
   line per skipped row.
5. If rows were skipped, send the warning lines to the clinical team. They fix the file (or add the
   **ensembl_id** column), and you run the DAG again with the new file.

To remove a panel, upload a file without it. This DAG has no delete mode and no replay mode.

## Parameters

| Param | Default | Meaning |
|:--|:--|:--|
| **tenant** | required | Tenant code, used in the path of the call. The panels change in this tenant only |
| **gene\_panel\_filepath** | required | S3 URI of the .tsv file |
| **strict** | false | true rejects the whole file when a row matches no Ensembl gene. false skips the row and logs a warning |

Example run config: {"tenant": "radiant", "gene\_panel\_filepath": "s3://&lt;bucket&gt;/gene\_panels/radiant/gene\_panels.tsv", "strict": false}

## File format

The portal owns the format; the DAG does not read the file.

| | |
|:--|:--|
| **Encoding** | UTF-8, tab-separated, max 10 MiB |
| **Header row** | **panel\_code**, **panel\_name**, **symbol**, and an optional **ensembl\_id** |
| **Rows** | One row per gene, many panels per file |
| **Filter value** | The **panel\_name**: it must be unique in the file, and saved filters use it |

When a row has both a symbol and an Ensembl ID, the ID wins.

## Results

| Portal answer | Meaning | Task outcome |
|:--|:--|:--|
| **200** | Panels replaced. Skipped rows (unknown gene) are logged as warnings, with their line, panel and symbol | success |
| **400** | The file is not valid: bad layout, bad panel\_code, two panels with the same panel\_name, empty or duplicate symbol. The detail has the line | fails, no retry. Fix the file |
| **403** | The account has no access to this tenant, or no **can\_manage\_analysis\_catalog** action | fails, no retry. See Authentication |
| **409** | A panel\_code of the file is already used by another panel of the tenant (for example a prescription panel) | fails, no retry. Change the code |
| **413** | The file is larger than 10 MiB | fails, no retry |
| **422** | strict is true and some rows match no gene. The rows are logged as warnings. Nothing changed | fails, no retry |
| **5xx**, network error | The portal or the network failed | retried 2 times, then fails. A retry is safe |

On a 400, 409, 413 or 422 the tenant's previous panels stay unchanged: the change is all or nothing.

## Authentication

Same **radiant\_api\_conn** Airflow Connection as the post-processing DAGs:

| Field | Value |
|:--|:--|
| **host** | API base url |
| **login** | OIDC client id |
| **password** | OIDC client secret |
| **extra** | A JSON object with token\_url and scope |

> **A valid token is not sufficient.** The portal checks its own permission store. The
> service-account user needs access to the tenant and the **can\_manage\_analysis\_catalog** action
> in that tenant. This is not the **ingest\_data** action that the post-processing DAGs use.

## Runtime

The task runs in the Airflow worker, with the same small portal client as the other portal calls
(requests, no extra package). It reads the file from S3 with the worker's AWS credentials, so the
worker needs read access to the S3 prefix of the file.

Before the download, the task checks the file: a missing or unreadable object, or one larger than
10 MiB, fails the task at once, without a retry. Check the path.

> **The task reads any object the worker can read.** A 400 from the portal can show part of the
> file in its detail (for example an unknown header column), and the log keeps it. Give only
> trusted operators the right to trigger this DAG, and keep the gene panel files in their own
> S3 prefix.
