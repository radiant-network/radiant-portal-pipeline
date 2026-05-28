# Load Exomiser Files

Loads Exomiser prediction TSV files into the staging exomiser table using a **partition swap** strategy.

## Swap Strategy

1. **Copy phase**: Copies existing staging exomiser records from the partition, excluding the current sequencing experiment IDs (`staging_exomiser_insert_partition_delta.sql`).
2. **Load phase**: For each sequencing experiment, loads the Exomiser TSV file via StarRocks `LOAD` (`staging_exomiser_load.sql`).

## Load Details

- Ingests TSV files with header row skipped
- Filters to **contributing variants only** (`contributing_variant=1`)
- Maps columns to staging table fields including: `rank`, `id`, `symbol`, `moi`, `gene_combined_score`, `variant_score`, `acmg_classification`, `acmg_evidence`, genomic coordinates, etc.
- `acmg_classification` is lowercased; `acmg_evidence` is split into an array
- `part` and `seq_id` are set from task parameters
