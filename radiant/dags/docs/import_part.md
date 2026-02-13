# Importing a partition in Radiant

This DAG processes a single partition of sequencing experiments through the Radiant pipeline.

## Overview

Given a partition number, this DAG:

1. **Fetches** the sequencing experiment delta for the partition from StarRocks (new or updated records)
2. **Imports VCF files** — triggers sub-DAGs and ECS/K8s tasks to ingest germline SNV and CNV VCF data into Iceberg
3. **Loads Exomiser** predictions from TSV files into a staging table, then inserts scored results joined with variant data
4. **Refreshes** Iceberg external tables in StarRocks so newly ingested data is visible
5. **Inserts CNV occurrences** — enriched with cytobands, gene overlaps, and gnomAD SV data (gated by a short-circuit check for CNV task types)
6. **Inserts variant hashes** into the variant lookup table for deduplication
7. **Builds SNV occurrences** — loads occurrence data enriched with exomiser scores into the partition via a swap strategy
8. **Computes variant frequencies** — calculates per-locus patient counts and frequency ratios, staged per partition then aggregated globally
9. **Builds SNV variants** — stages variants with public annotations (gnomAD, ClinVar, TopMed, dbSNP, OMIM), merges with frequencies, and writes to the partitioned variant table
10. **Builds SNV consequences** — enriches consequences with protein prediction scores and constraint metrics, then builds a filtered/partitioned consequence table
11. **Cleans up** — deletes sequencing experiments for removed tasks and marks processed tasks with an `ingested_at` timestamp

## Parameters

| Parameter | Description |
|-----------|-------------|
| `part`    | Integer representing the partition to process |

## Notes

- The DAG supports both AWS (ECS) and non-AWS (K8s) execution environments.
- Partition swap operators (`RadiantStarRocksPartitionSwapOperator`) first copy existing partition data excluding the current task IDs, then insert the new delta, ensuring atomicity.
- Tasks with `NONE_FAILED` or `NONE_FAILED_MIN_ONE_SUCCESS` trigger rules allow downstream processing to continue even when optional branches (e.g., CNV, delta SNV) are skipped.
- Short-circuit operators gate the CNV branch (requires `ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK` tasks) and the delta SNV branch (requires `RADIANT_GERMLINE_ANNOTATION_TASK` tasks that are not deleted).
