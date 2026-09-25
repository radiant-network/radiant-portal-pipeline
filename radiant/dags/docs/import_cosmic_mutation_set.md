# Importing the COSMIC Mutation Census

Loads the COSMIC Mutation Census (`cmc_export.tsv.gz`) into the StarRocks table **cosmic_mutation_set**:
one row per variant with the columns the legacy Spark ETL exposed as the `cmc` struct (`mutation_url`,
`shared_aa`, `cosmic_id`, `sample_mutated`, `sample_tested`, `tier`, `sample_ratio`), keyed on `locus_id`.
Manual trigger only: COSMIC is licensed and never published by OpenDataLake, so the file has to be
fetched and uploaded by hand.

---

## Why the variants are normalized first

COSMIC does not write variants the way a VCF does. An insertion has an **empty reference allele** and a
position spanning the two flanking bases (`1:26731451-26731452`, alt `C`); a deletion has an **empty
alternate allele** over the deleted range (`20:32434646-32434646`, ref `G`). The pipeline keys every
variant on a VCF-style record (anchor base included, left-aligned), so a locus built from the COSMIC
columns as published never matches an indel — which is how the previous ETL silently lost them.

The **normalize** task rebuilds each row as a VCF record with the anchor base read from the GRCh38
reference, runs `bcftools norm` against that same reference, and writes the rows back to S3 keyed on the
normalized alleles. For example:

| COSMIC id | As published (GRCh38) | After normalization |
|:--|:--|:--|
| COSV61373102 | `1:26731451-26731452`, ref ``, alt `C` | `1-26731445-G-GC` |
| COSV60102180 | `20:32434646-32434646`, ref `G`, alt `` | `20-32434638-AG-A` |

Rows without a GRCh38 position, on a contig the reference lacks, or whose reference allele contradicts
the FASTA are dropped and counted; the task's XCom carries the counts (`rows_in`, `rows_out`, one counter
per drop reason, and bcftools's own line summary).

## Input

| Param | Type | Description |
|:--|:--|:--|
| **cosmic_mutation_set_filepath** | string, required | S3 path of `cmc_export.tsv.gz`, the census download unchanged (header row included, gzip). The task reads the columns **by header name**, so column order does not matter. |
| **reference_fasta_filepath** | string | S3 path of the GRCh38 FASTA to left-align against; its `.fai` index must sit next to it (`<fasta>.fai`). Defaults to the `RADIANT_REFERENCE_FASTA_S3_URI` environment variable of the Airflow deployment. Use the same reference the VCFs were called on, so both sides spell the contigs the same way. |
| **normalized_filepath** | string, optional | Where the normalized TSV is written and then loaded from. Empty means next to the input, with the `.tsv.gz` extension replaced by `.normalized.tsv.gz`. The task role must be able to **write** there. |

```json
{
  "cosmic_mutation_set_filepath": "s3://<bucket>/raw/landing/cosmic/cmc_export.tsv.gz",
  "reference_fasta_filepath": "s3://<bucket>/reference/Homo_sapiens_assembly38.fasta"
}
```

## Steps

| # | Task | Where | What it does |
|:--|:--|:--|:--|
| 1 | **resolve_normalized_filepath** | Airflow | Decides the output path and refuses to start without a reference FASTA. |
| 2 | **normalize_cosmic_mutation_set** | radiant-operator pod (K8s) or task (ECS) | Downloads the export and the FASTA (~3 GB), rebuilds the rows as VCF records, `bcftools norm`, writes the keyed TSV to S3. Needs ~8 GB of scratch disk; ~15 minutes. |
| 3 | **load_raw_cosmic_mutation_set** | StarRocks | `TRUNCATE` then `BROKER LOAD` the normalized TSV into the staging table `raw_cosmic_mutation_set` (one row per mutation × transcript, as published). |
| 4 | **insert_cosmic_mutation_set_hashes** | StarRocks | Registers in `variant_lookup` the loci the `GET_VARIANT_ID` UDF cannot encode, like every other variant-level source. |
| 5 | **insert_cosmic_mutation_set** | StarRocks | `INSERT OVERWRITE` cosmic_mutation_set: resolves `locus_id`, keeps one row per locus (the highest `sample_mutated`, as the legacy ETL did) and computes `sample_ratio`. |

The truncate and the load are two statements, so the staging table is empty for the duration of the
load and stays empty if it fails; `cosmic_mutation_set` itself is only replaced by the final `INSERT
OVERWRITE`. Re-run the DAG with the same params to recover.

## Triggered from radiant-import-open-data

**radiant-import-open-data** accepts `cosmic_mutation_set_filepath` (and an optional
`reference_fasta_filepath`). When the filepath is set, the import triggers this DAG after its other sources
and waits for it; when it is left empty, the branch is skipped — the same gating as the Cancer Gene
Census, the ClinVar RCV summary and the cytoband loads. The weekly re-annotation never sets it.
