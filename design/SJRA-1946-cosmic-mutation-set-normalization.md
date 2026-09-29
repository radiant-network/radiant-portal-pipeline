# SJRA-1946 — COSMIC Mutation Census: `cosmic_mutation_set` with variant normalization

## Problem

The portal needs the COSMIC Mutation Census (`cmc_export.tsv.gz`, ~5.8M rows, one per mutation × transcript)
as a variant-level StarRocks table with the columns the legacy Spark ETL exposed as the `cmc` struct
(`datalake-lib` `enriched/Variants.scala`, `withCosmic`): `mutation_url`, `shared_aa`, `cosmic_id`,
`sample_mutated`, `sample_tested`, `tier`, `sample_ratio`, one row per locus keeping the highest
`sample_mutated`.

The legacy ETL (`publictables/normalized/cosmic/CosmicMutationSet.scala`) keyed the rows on the COSMIC
columns as published and lost indels in the join. Profiling the export shows why, and it is not a
left-alignment subtlety but the encoding itself:

| Kind | COSMIC row | VCF record |
|:--|:--|:--|
| Insertion | position `1:26731451-26731452` (the two flanking bases), ref **empty**, alt `C` | `chr1 26731451 C CC`, left-aligned to `26731445 G GC` |
| Deletion | position `20:32434646-32434646` (the deleted bases), ref `G`, alt **empty** | `chr20 32434645 GG G`, left-aligned to `32434638 AG A` |
| SNV / MNV / delins | both alleles present | as is; bcftools trims shared bases |

A `chrom-start-ref-alt` built from the COSMIC columns therefore never equals a VCF-derived locus for
any indel: no anchor base, a different start, and no left-alignment.

## Design

`radiant-import-cosmic-mutation-set` (manual, also triggered from `radiant-import-open-data` when
`cosmic_mutation_set_filepath` is set):

1. **normalize** — one task in the radiant-operator image (Kubernetes `@task.kubernetes` or ECS
   `scripts/ecs/normalize_cosmic_mutation_set.py`), `radiant/tasks/open_data/cosmic_mutation_set.py`:
   - streams the export, resolving columns by header name;
   - rebuilds each row as a VCF record, reading the anchor base from the GRCh38 FASTA through a small
     `.fai` reader (no pysam), and checking COSMIC's reference allele against the FASTA;
   - `bcftools norm --fasta-ref --check-ref wx` (bcftools is apt-installed in
     `Dockerfile.radiant.operator`), then `bcftools query` and an external `sort` on the record ID —
     required because `norm` reorders records through its realignment window;
   - a second streaming pass merges the normalized alleles back onto the rows by ID and writes a gzipped
     TSV keyed on `chromosome, start, reference, alternate, locus_hash` (the pipeline's own
     `locus_and_hash`, `radiant/tasks/vcf/snv/common.py`), uploaded next to the input;
   - returns counts (`rows_in`, `rows_out`, per-reason drops, bcftools's line summary) and refuses to
     finish if they do not add up.
2. **load_raw_cosmic_mutation_set** — truncate + broker load into the staging table.
3. **insert_cosmic_mutation_set_hashes** — `variant_lookup` registration for loci the `GET_VARIANT_ID`
   UDF cannot pack, like every other variant-level source.
4. **insert_cosmic_mutation_set** — `INSERT OVERWRITE` with `ROW_NUMBER() OVER (PARTITION BY locus_hash
   ORDER BY sample_mutated DESC)`.

## Decisions

- **bcftools in the operator image rather than a biocontainer step.** A separate `quay.io/biocontainers`
  pod would need an S3 sidecar on Kubernetes and a new multi-container ECS task definition; adding the
  Debian package to the image we already run everywhere keeps one task, one image and no Terraform change.
- **Reference FASTA from S3.** `RADIANT_REFERENCE_FASTA_S3_URI` is the DAG param default; the `.fai` must
  sit next to the FASTA. Use the same reference the VCFs were called on so contigs are spelled the same:
  the module maps `1`→`chr1` when the FASTA is prefixed and strips the prefix on the way back exactly as
  the SNV extraction does, so `chrM` yields `M` on both sides.
- **Final table = cmc columns only**, keyed on `locus_id`; the staging table keeps the normalized rows
  (with `locus_hash`) for the load and for debugging. `mutation_url` is stored as published — the Scala
  appended `&genome=37`, a parameter the URL already carries. `tier` stays a string (`1`, `2`, `3`,
  `Other`); `Other` is a real category, not a missing value.
- **Rows dropped, counted, never guessed:** no GRCh38 position (~0.9% of the export), a contig the
  FASTA lacks, a span that disagrees with the allele, a reference allele the FASTA contradicts.
- **Strand-flipped liftover rows stay dropped (decided 2026-09-25).** 11,401 of the 11,513 reference
  mismatches are exact reverse complements, clustered in 80 genes in segments GRCh38 inverted relative to
  GRCh37 (PDE4DIP, NBPF10, 1q21, 10q11, Xq28, 22q11): COSMIC lifted the positions but kept the GRCh37-strand
  alleles. Recovering them by reverse-complementing both alleles would assert a diagnosis COSMIC itself does
  not make, a single-base complement match is a coincidence one time in three, and a liftover into a
  segmental duplication may sit on the wrong paralog anyway. They are counted under ``rows_ref_mismatch``.

## Verified

`COSV61373102` (ARID1A c.1650dup) → `1-26731445-G-GC` and `COSV60102180` (ASXL1 c.1934del) →
`20-32434638-AG-A`, both run through the module against the hg38 sequence of the region.
