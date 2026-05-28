# Germline SNV Variant Frequency

Variant frequency computation happens in two stages: per-partition staging, then global aggregation.

## Stage 1: Insert Staging Variant Frequency (per partition)

**SQL**: `germline_snv_staging_variant_freq_insert.sql`

Calculates per-locus patient counts for the current partition, stratified by:

- **Experimental strategy**: WGS vs WXS
- **Affected status**: affected vs non-affected

### Metrics

| Metric | Description |
|---|---|
| `pc_*` (patient count) | Number of distinct patients carrying the variant at this locus |
| `pn_*` (patient total) | Total number of distinct patients in the partition for the given strategy/status |

### Quality Filters

Only occurrences meeting these thresholds are counted:
- `gq >= 20`
- `filter = 'PASS'`
- `ad_alt > 3`

## Stage 2: Aggregate Variant Frequencies (global)

**SQL**: `germline_snv_variant_frequency_insert.sql`

Aggregates staging frequencies across **all partitions** into the final `variant_frequency` table:

- Sums `pc_*` (patient counts) across partitions per `locus_id`
- Sums `pn_*` (patient totals) across partitions (using `ANY_VALUE` per partition, then summing)
- Computes frequency ratios: `pf_* = pc_* / pn_*`

### Output Columns

For each of the 6 strata (WGS, WGS affected, WGS not affected, WXS, WXS affected, WXS not affected):
- `pc_*` — patient count
- `pn_*` — patient number (total)
- `pf_*` — patient frequency (ratio)
