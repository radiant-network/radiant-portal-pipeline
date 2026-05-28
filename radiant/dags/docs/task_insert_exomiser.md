# Insert Exomiser

Inserts scored Exomiser results into the final exomiser table by joining staging data with variant information.

## Logic

- Joins `staging_exomiser` with `germline_snv_tmp_variant` on `locus_hash` to resolve `locus_id`
- Computes `variant_rank` using `ROW_NUMBER()` partitioned by `(locus_hash, seq_id)` and ordered by `gene_combined_score DESC` — this ranks variants per gene score so downstream tasks can pick the top-ranked exomiser result
- Filtered by the current partition (`part`)
- Uses `INSERT OVERWRITE` with dynamic overwrite to replace only the affected partition data

## Trigger Rule

Runs with `NONE_FAILED` — proceeds even if upstream optional branches were skipped.
