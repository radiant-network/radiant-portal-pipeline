# Germline SNV Consequence

The consequence task group builds the final germline SNV consequence tables through a three-step pipeline.

## Step 1: Insert Consequences

**SQL**: `germline_snv_consequence_insert.sql`

Enriches consequence data from Iceberg with prediction scores and constraint metrics:

| Source | Fields |
|---|---|
| **dbNSFP** | `sift_score`, `sift_pred`, `polyphen2_hvar_score`, `polyphen2_hvar_pred`, `fathmm_score`, `fathmm_pred`, `cadd_score`, `cadd_phred`, `dann_score`, `revel_score`, `lrt_score`, `lrt_pred`, `phyloP17way_primate`, `phyloP100way_vertebrate` |
| **SpliceAI** | `spliceai_ds`, `spliceai_type` |
| **gnomAD Constraint** | `pli`, `loeuf` |

Joins are performed on `locus_id` + `transcript_id` (for dbNSFP), `locus_id` + `symbol` (for SpliceAI), and `transcript_id` (for gnomAD constraint).

Filtered by the current task IDs.

## Step 2: Insert Consequence Filter

**SQL**: `germline_snv_consequence_filter_insert.sql`

Builds a filtered/flattened view of consequences:

- **Unnests** the `consequences` array so each consequence type gets its own row
- **Aggregates** by `(locus_id, consequence, symbol, sift_score, polyphen2_hvar_score, fathmm_score, revel_score, spliceai_ds, impact_score, gnomad_pli, vep_impact)`
- **Marks `is_deleterious`**: `true` if at least one prediction score (SIFT, FATHMM, PolyPhen2, CADD, DANN, LRT, REVEL, phyloP primate, phyloP vertebrate, SpliceAI) is non-null
- Uses `INSERT OVERWRITE` to replace the full consequence filter table

## Step 3: Insert Consequence Filter Part (partitioned)

**SQL**: `germline_snv_consequence_filter_insert_part.sql`

Copies filtered consequences into the partitioned table for the current partition. Only consequences with matching occurrences in the partition are included.
