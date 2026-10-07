# Import Tenant Gene Panels

Sets the genes of the gene panels of one tenant from one TSV file on S3. The DAG reads the file and
attaches it, unchanged, to one portal call: **PUT /{tenant}/gene_panels**. The portal parses and
checks the file, finds the Ensembl gene of each row, replaces the tenant's panels in one
transaction, then refreshes the tenant's gene panel materialized view. The new genes are available
for variant filtering when the call returns.

**The file is the full list.** For each panel code of the file:

- A panel of the tenant with the same code (any case), for example a panel of the analysis catalog
  (prescriptions), keeps its name and settings and gets the genes of the file.
- A new code creates an uploaded panel. Its name is its code.

A panel of the tenant that is not in the file loses all its genes. If an earlier upload created
it, it is removed. Sending the same file again gives the same result, so a re-run or a retry is
safe. Only one run at a time (max\_active\_runs is 1).

---

## Runbook

1. Get the TSV file from the clinical team. Check that it holds **all** the tenant's panels, not
   only the new ones, and the catalog panels too.
2. Put it in the tenant's S3 prefix, for example
   s3://&lt;bucket&gt;/gene_panels/radiant/gene_panels.tsv.
3. Trigger this DAG with the parameters below.
4. Read the log of **upload_gene_panels**: one line with the panel and gene counts, and one warning
   line per row warning.
5. If there are warnings, send them to the clinical team. They fix the file (for example, they put
   the Ensembl gene ID in the **symbol** column), and you run the DAG again with the new file.

To remove a panel, upload a file without it. This DAG has no delete mode and no replay mode.

## Parameters

| Param | Default | Meaning |
|:--|:--|:--|
| **tenant** | required | Tenant code, used in the path of the call. No space. The panels change in this tenant only |
| **gene\_panel\_filepath** | required | S3 URI of the .tsv file (s3://&lt;bucket&gt;/&lt;key&gt;, not a prefix) |
| **strict** | false | true rejects the whole file when a row matches no Ensembl gene. false skips the row and logs a warning |

Example run config: {"tenant": "radiant", "gene\_panel\_filepath": "s3://&lt;bucket&gt;/gene\_panels/radiant/gene\_panels.tsv", "strict": false}

## File format

The portal owns the format; the DAG does not read the file.

| | |
|:--|:--|
| **Encoding** | UTF-8 (a byte order mark is accepted), tab-separated, max 10 MiB |
| **Header row** | Must have a **symbol** column and a **panels** column (any case, any order). Other columns, for example **version**, are ignored |
| **Rows** | One row per gene. Each row has the same number of columns as the header |
| **symbol** | The gene symbol, or its Ensembl gene ID. Unique in the file (any case) |
| **panels** | The comma-separated codes of the panels the gene is in. A code has 1 to 50 characters: letters, digits, \_ and -, and starts with a letter or a digit |
| **Filter value** | The panel name: the code for a new panel, the existing name for a catalog panel |

A symbol is looked up as a gene name first, then as an Ensembl gene ID. A symbol with more than one
Ensembl gene adds all of them.

Example:

    symbol	panels	version
    BRCA1	ONCO,HBOC	2
    TP53	ONCO	2

## Results

| Portal answer | Meaning | Task outcome |
|:--|:--|:--|
| **200** | Panels replaced. Row warnings are logged, with their line, symbol and message (see below) | success |
| **400** | The file is not valid: empty file, missing or duplicate column, wrong column count, empty or duplicate symbol, bad panel code, two codes that differ only by case, no gene row, no panel. The detail has the line | fails, no retry. Fix the file |
| **403** | The account has no access to this tenant, or no **can\_manage\_analysis\_catalog** action | fails, no retry. See Authentication |
| **404** | The portal has no gene panel endpoint (not deployed) or no such tenant | fails, no retry |
| **409** | An uploaded panel that is not in the file is still used by the analysis catalog, so it cannot be removed | fails, no retry. Add the panel to the file |
| **413** | The request is larger than 10 MiB | fails, no retry |
| **422** | strict is true and some rows match no Ensembl gene. The rows are logged as warnings. Nothing changed | fails, no retry |
| **408**, **429**, **5xx**, network error | The portal or the network failed | retried 2 times, then fails. A retry is safe |

On a 400, 409, 413 or 422 the tenant's previous panels stay unchanged: the change is all or nothing.
A 500 can also come after the commit, when the materialized view refresh fails: the panels are
saved, and the retry sends the same file again.

Row warnings on a 200:

| Warning | Effect |
|:--|:--|
| symbol matches no Ensembl gene | Row skipped (with strict, 422 instead) |
| Ensembl gene X is named Y | Row kept, with the Ensembl name Y |
| same Ensembl gene X as line N | Row skipped in that panel: two rows give the same gene |

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

Before the download, the task checks the file: a missing object, no read access, or an object
larger than 10 MiB fails the task at once, without a retry. Check the path. Other S3 errors are
retried.

The task returns the counts (panels, genes, warnings) as its XCom.

> **The task reads any object the worker can read.** A 400 from the portal can show part of the
> file in its detail (for example a bad panel code), and the log keeps it. Give only trusted
> operators the right to trigger this DAG, and keep the gene panel files in their own S3 prefix.
