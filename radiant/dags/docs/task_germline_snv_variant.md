# Germline SNV Variant

The variant task group builds the final germline SNV variant tables through a multi-step pipeline.

## Step 1: Insert Staging Variants

**SQL**: `germline_snv_staging_variant_insert.sql`

Enriches variants from `germline_snv_tmp_variant` with public database annotations:

| Source | Fields |
|---|---|
| **gnomAD Genomes v3** | `gnomad_v3_af` (allele frequency) |
| **TopMed BRAVO** | `topmed_af` |
| **1000 Genomes** | `tg_af` |
| **ClinVar** | `clinvar_name`, `clinvar_interpretation` |
| **dbSNP** | `rsnumber` |
| **OMIM Gene Panel** | `omim_inheritance_code` (aggregated unique codes per gene symbol) |

Also carries forward variant metadata: `chromosome`, `start`, `end`, `variant_class`, `symbol`, `impact_score`, `consequences`, `vep_impact`, MANE/canonical flags, HGVS notations, `locus`, `dna_change`, `aa_change`, `transcript_id`.

### Gating

This step is gated by the `sanity_check_delta_snv` short-circuit — only runs if non-deleted `RADIANT_GERMLINE_ANNOTATION_TASK` tasks exist.

## Step 2: Insert Variants with Frequencies

**SQL**: `germline_snv_variant_insert.sql`

Merges staging variant data with the aggregated variant frequency table:

- Joins `staging_variant` with `variant_frequency` on `locus_id`
- Defaults all missing frequency values to `0` using `COALESCE`
- Uses `INSERT OVERWRITE` to replace the entire `germline_snv_variant` table

## Step 3: Insert Variants Part (partitioned)

**SQL**: `germline_snv_variant_part_insert_part.sql`

Copies variants into the partitioned variant table for the partition range computed by `compute_parts`:

- `variant_part = part // 10`
- `part_lower = variant_part * 10`
- `part_upper = (variant_part + 1) * 10`

Only variants with matching occurrences in the partition range are included.
