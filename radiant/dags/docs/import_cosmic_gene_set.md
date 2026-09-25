# Importing the COSMIC Cancer Gene Census

Loads the COSMIC Cancer Gene Census TSV from S3 into the StarRocks table **cosmic_gene_set**, then
rebuilds **cosmic_gene_panel** from it. Manual trigger only: COSMIC is licensed and never published
by OpenDataLake, so the file has to be fetched and uploaded by hand.

---

## Input

One DAG param, **cosmic_gene_set_filepath**: a list of S3 paths to
`Cosmic_CancerGeneCensus_GRCh38.tsv.gz` (gzip is detected from the extension; a plain `.tsv` works
too). The file is the census download from COSMIC, unchanged, header row included.

```json
{"cosmic_gene_set_filepath": ["s3://<bucket>/raw/landing/cosmic/Cosmic_CancerGeneCensus_GRCh38.tsv.gz"]}
```

The columns are read **by position**, in the census header order (see the comment at the top of
`sql/open_data/cosmic_gene_set_load.sql`). A COSMIC release that reorders or adds columns needs that
placeholder list updated first.

## Steps

| # | Task | What it does |
|:--|:--|:--|
| 1 | **load_cosmic_gene_set** | `TRUNCATE` then `BROKER LOAD` into cosmic_gene_set. Flags become booleans, comma lists become trimmed arrays, empty cells become NULL — the same shape the legacy Spark ETL produced. |
| 2 | **insert_cosmic_gene_panel** | `INSERT OVERWRITE` cosmic_gene_panel: one row per gene and germline tumour type. |

The truncate and the load are two statements, so the table is empty for the duration of the load
and stays empty if it fails. Re-run the DAG with the same param to recover.

## Triggered from radiant-import-open-data

**radiant-import-open-data** accepts the same param. When it is set, the import triggers this DAG
after its other sources and waits for it; when it is left empty, the branch is skipped — the same
gating as its ClinVar RCV summary and cytoband loads. The weekly re-annotation never sets it.
