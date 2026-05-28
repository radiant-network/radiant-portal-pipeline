# Insert Germline SNV Occurrences

Inserts germline SNV occurrence data into the partition using a **partition swap** strategy.

## Swap Strategy

1. **Copy phase**: Copies existing records from the current partition, excluding the task IDs being processed (`germline_snv_occurrence_copy_partition.sql`).
2. **Insert phase**: Inserts the new delta from Iceberg (`germline_snv_occurrence_insert_partition_delta.sql`).

## Data Enrichment

The insert query selects from `iceberg_occurrence` and enriches each SNV occurrence with:

| Field | Source | Description |
|---|---|---|
| `locus_id` | `germline_snv_tmp_variant` | Resolved by joining on `locus_hash` |
| Exomiser scores | `exomiser` table | `exomiser_moi`, `exomiser_acmg_classification`, `exomiser_acmg_evidence`, `exomiser_variant_score`, `exomiser_gene_combined_score` — only the top-ranked variant per locus (`variant_rank = 1`) |

## Filters

- Only occurrences where `has_alt = true`
- Filtered by the current partition and task IDs

## Additional Fields

Includes genotype quality metrics (`gq`, `dp`, `ad_ratio`, `ad_ref`, `ad_alt`, `ad_total`), zygosity, parental origin, father/mother genotype data, transmission mode, phasing, and VCF info fields.

## Trigger Rule

Runs with `NONE_FAILED` — proceeds even if upstream optional branches were skipped.
