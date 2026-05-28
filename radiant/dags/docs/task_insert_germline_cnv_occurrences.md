# Insert Germline CNV Occurrences

Inserts germline CNV occurrence data into the partition using a **partition swap** strategy for atomicity.

## Swap Strategy

1. **Copy phase**: Copies existing records from the current partition, excluding the sequencing experiment IDs being processed (`germline_cnv_occurrence_copy_partition.sql`).
2. **Insert phase**: Inserts the new delta from Iceberg (`germline_cnv_occurrence_insert_partition_delta.sql`).

## Data Enrichment

The insert query enriches raw CNV occurrences from Iceberg with:

| Enrichment | Source | Description |
|---|---|---|
| **Cytobands** | `cytoband` table | Aggregates overlapping cytobands based on chromosome and positional overlap |
| **Genes** | `ensembl_gene` table | Aggregates overlapping gene symbols and counts (`nb_genes`) |
| **SNV count** | `iceberg_occurrence` table | Counts heterozygous/homozygous SNVs (`has_alt=true`) within the CNV boundaries for the same sequencing experiment |
| **gnomAD SV** | `iceberg_gnomad_sv` table | Matches gnomAD structural variants with **>=80% reciprocal overlap**, same alternate allele, `PASS` filter, and `DUP`/`DEL` svtype. Ranked by allele frequency (top 1 match kept). Provides `gnomad_af`, `gnomad_sc`, `gnomad_sn`, `gnomad_sf`, `gnomad_sc_hom`, `gnomad_sc_het` |

## Gating

This task is gated by the `sanity_check_cnvs` short-circuit: it only runs if the partition contains tasks of type `ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK`.
