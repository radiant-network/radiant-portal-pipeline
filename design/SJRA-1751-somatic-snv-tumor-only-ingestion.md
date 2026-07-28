# SJRA-1751 — Technical Analysis: Somatic SNV Ingestion (Tumor-Only)

---

## 1. Overview

This document describes the technical design for ingesting **somatic Tumor-Only (TO)**
SNV variants into the Radiant portal pipeline.

It is the direct continuation of
[SJRA-341 — Somatic SNV Ingestion (Tumor-Normal)](./SJRA-341-somatic-snv-ingestion.md),
which built the Tumor-Normal (TN) path and explicitly deferred Tumor-Only (§7 of that
document: *"Tumor-only analysis (no matched normal): not covered in this design"* and
*"Tumor-only frequency columns (`pc_to_*`): deferred, columns documented in §4.7 for
future implementation"*).

**Input**: annotated somatic VCF (VEP `CSQ`, same shape as germline / somatic TN), one
tumor sample per file, no matched normal.
**Output**: TO occurrences queryable in StarRocks, with TO variant frequencies
(`pc_to_*` / `pn_to_*` / `pf_to_*`) computed alongside the existing TN frequencies.

**Scope**: ETL only. API and UI are out of scope. CNV is out of scope. WES (`wxs`) and
WGS (`wgs`) must both be supported.

Also in scope:

- **Hotspot must be handled as a boolean.** The tumor-only VCF marks known somatic sites with
  a presence/absence flag, not an allele index. The existing `info_hotspotallele` column is
  never populated today because the pipeline looks for a differently-named key. Read the flag
  into that column and treat it as boolean throughout — this matches QLIN, which also models
  hotspot as a boolean. Details in §7.3.
- **Capture the somatic quality score.** The tumor-only caller reports a per-call somatic
  confidence value and emits no germline-style quality field, so without it a TO call carries
  no quality signal at all. Required for QLIN parity. Details in §7.3.

---

## 2. What Already Exists

A significant amount of TO groundwork is already in the codebase — SJRA-341 laid the
columns in but wired them to constants.

| Asset | State | Location |
|---|---|---|
| `pc_to_wgs` / `pn_to_wgs` / `pf_to_wgs` / `pc_to_wxs` / `pn_to_wxs` / `pf_to_wxs` | **Columns exist** in both the staging and the per-tenant frequency tables | `init/somatic_snv_staging_variant_frequency_create_table.sql:11-16`, `init/somatic_snv_variant_frequency_create_table.sql:9-14` |
| TO frequency values | **Hardcoded to `0`** | `somatic_snv_staging_variant_freq_insert.sql:57-62` |
| Per-tenant TO rollup | **Already implemented** — sums `pc_to_*`, takes `ANY_VALUE(pn_to_*)` | `somatic_snv_variant_frequency_insert.sql:24-25, 37-42` |
| `somatic__snv__occurrence` normal columns | **All nullable**; only `tumor_seq_id` is `NOT NULL` | `init/somatic_snv_occurrence_create_table.sql:6, 46-57` |
| `is_tumor_normal` flag | **Computed but only used to filter the TN denominator** | `somatic_snv_staging_variant_freq_insert.sql:11-13, 30` |

The consequence is that **level 2 of the frequency chain needs no change at all** (§8.2),
and the occurrence table is already physically able to hold a TO row.

### 2.1 The one hard blocker

`radiant/tasks/vcf/snv/somatic/process.py:145-149` rejects any task without a normal
sample:

```python
if tumor_index is None or normal_index is None:
    raise ValueError(
        f"Could not find both tumor and normal samples [{samples}] "
        f"in the VCF for the given experiments: {experiments}."
    )
```

This is the single line that makes TO impossible today.

---

## 3. Current Tumor-Normal Flow (Reference)

```
import_part.py
  ├── fetch_sequencing_experiment_delta          → tasks (RadiantSomaticAnnotationTask)
  ├── import_somatic_snv_vcf (k8s/ECS operator)
  │     └── process_task(task)                   radiant/tasks/vcf/snv/somatic/process.py
  │           ├── get_sorted_task_experiments()  ← resolves tumor_index / normal_index
  │           ├── process_common()               → locus_hash, chrom, start, ref, alt
  │           ├── process_occurrence()           → somatic_snv_occurrence (Iceberg)
  │           ├── process_variant()              → snv_variant       (Iceberg, SHARED)
  │           └── process_consequence()          → snv_consequence   (Iceberg, SHARED)
  ├── refresh_iceberg_tables
  └── TaskGroup somatic_snv_occurrence           (per tenant)
        ├── insert_somatic_snv_occurrences       Iceberg → somatic__snv__occurrence (partition swap)
        ├── insert_stg_somatic_snv_variant_freq  → somatic__snv__staging_variant_frequency_part
        └── aggregate_somatic_snv_variant_freq   → somatic__snv__variant_frequency
              ↓ (later, pooled across ALL tenants)
        tg_variants → snv__variant.somatic_pc_tn_* / somatic_pf_tn_*
```

Note the split of ownership: `somatic__snv__occurrence` is somatic-specific, while
`snv_variant` / `snv_consequence` are **shared with germline** and pooled across tenants.

---

## 4. Input VCF — Characterisation

Reference file: `21305.dragen.WES_somatic-tumor_only.hard-filtered.norm.VEP.vcf.gz`
(39 MB gzipped).

- **Caller**: DRAGEN `07.021.624.3.10.8`, invoked with `--tumor-fastq-list-sample-id`
  and no normal — a genuine tumor-only run.
- **Assay**: WES, Roche KAPA HyperExome targets, hg38 (`hg38_alt_masked_graph_v2`).
- **Annotation**: VEP v103, merged cache, GENCODE 37, gnomAD r2.1.
- **Pre-processing**: `hard-filtered` + `norm` (left-aligned and split).
- **Samples**: exactly **1** (`21305`) — satisfies the TO validation in §7.1.
- **Records**: **474,589**, of which 470,485 on supported chromosomes.

### 4.1 Volume

| Stage | Rows per TO sample | Gate |
|---|---:|---|
| VCF records | 474,589 | — |
| On supported chromosomes | 470,485 | `SUPPORTED_CHROMOSOMES` — 4,104 skipped (`_alt`, `_random`, `chrUn_*`); 11 chrM records are kept |
| Multi-allelic (skipped) | **0** | already split by `norm` — the `len(record.ALT) <= 1` guard drops nothing |
| → `somatic_snv_occurrence` (Iceberg) | 470,485 | one row per record |
| → **`somatic__snv__occurrence` (StarRocks)** | **273,449** (57.6%) | `COALESCE(tumor_has_alt, FALSE)`, which after `adjust_somatic_calls_and_zygosity` is effectively `ad_alt >= 2` |
| → `snv_variant` (Iceberg, **SHARED**) | 470,485 | no gate |
| → `snv_consequence` (Iceberg, **SHARED**) | **1,361,615** | mean 2.9 CSQ entries/record, max 87 |
| Counted as carrier in frequency | 170,599 (35.9%) | `filter = 'PASS' AND tumor_ad_alt > 2` |

> ~1.36 M consequence rows and ~0.47 M variant rows are produced **per TO WES sample**.
> Note these are *write* volumes into the Iceberg task partitions, **not** catalog growth:
> the StarRocks `snv__variant` / `snv__consequence` tables are PRIMARY KEY (locus-keyed)
> and upsert, so they grow by novel loci only. See §7.5 for why this is a throughput
> consideration rather than a design problem.

### 4.2 The frequency filter is already satisfied by `PASS`

`PASS` records: 170,599. `PASS AND ad_alt > 2`: **170,599** — identical. DRAGEN's
`too_few_supporting_reads` filter already enforces the depth floor, so the
`tumor_ad_alt > 2` clause confirmed in §8 is redundant for this caller (harmless, and
worth keeping for callers that do not).

`FILTER` vocabulary is rich and multi-valued (max observed length 133 chars, fits
`VARCHAR(255)`):

| FILTER | Records |
|---|---:|
| `too_few_supporting_reads` | 220,534 |
| `PASS` | 170,599 |
| `too_few_supporting_reads;weak_evidence` | 47,828 |
| `weak_evidence` | 8,847 |
| `weak_evidence;systematic_noise` | 3,905 |
| … 14 further filter tokens | — |

### 4.3 A tumor-only VCF is overwhelmingly germline background

This is the single most consequential property of the input.

**VAF spectrum of PASS calls:**

| AF band | Records | |
|---|---:|---|
| `AF < 0.05` | 822 | plausible low-VAF somatic |
| `0.05 ≤ AF < 0.10` | 399 | |
| `0.10 ≤ AF < 0.25` | 740 | |
| `0.25 ≤ AF < 0.40` | 6,954 | |
| `0.40 ≤ AF < 0.60` | 63,635 | germline heterozygous peak |
| `AF ≥ 0.60` | 98,049 | germline homozygous peak |

**94.8 % of PASS calls sit at AF ≥ 0.40**, and **98.8 % of PASS calls (168,480 / 170,599)
carry the dbSNP `DB` flag.** Without a matched normal there is nothing to subtract, so the
file is the patient's germline exome plus a small somatic tail.

Two direct consequences:

1. **TO "frequencies" measure germline allele frequency, not somatic recurrence.** A
   `pf_to_wxs` near 0.5 for a common SNP is the expected, correct output of the design in
   §8 — it is not a bug, but consumers must not read it as somatic recurrence. QLIN has
   the same property.
2. It sharpens OQ-4 (§11) from a theoretical question into a real one.

### 4.4 `tumor_zygosity` is not meaningful for tumor-only

**Genotype distribution:**

| GT | Records |
|---|---:|
| `0/1` | 402,454 |
| `0\|1` | 71,436 |
| `1\|0` | 691 |
| `.\|.` / `./.` | 8 |

DRAGEN somatic does not genotype — it reports a variant and its VAF, emitting `0/1`
essentially always. Combined with §4.3, this means **the 98,049 germline-homozygous calls
(AF ≥ 0.60) are all recorded as `HET`**.

So `adjust_somatic_calls_and_zygosity` will yield `HET` for ~100 % of TO rows and `HOM`
for none. This does not crash anything, but it makes the column uninformative and it
interacts with the dbt assertion `somatic_snv_occurrence__validate_no_wt_tumor_zygosity`.
See §7.4 — this revises the earlier recommendation.

15.2 % of records are phased (`|` with a `PS` field), so `tumor_phased` will be populated
more often than in germline.

### 4.5 Field inventory vs. the current schema

Three-way comparison. **"Radiant column"** = do we have somewhere to put it today;
**"QLIN persists"** = does QLIN actually keep it, as distinct from merely reading it. That
distinction matters: QLIN's VCF input models are auto-generated from a sample file and declare
far more fields than survive its projections.

**INFO fields actually present** (full-file counts):

| INFO | In TO VCF | Radiant column | QLIN persists |
|---|---:|---|---|
| `DP` | 474,589 | ✅ `info_dp` | ❌ — its occurrence depth comes from `FORMAT/DP` |
| `MQ` | 474,589 | ✅ `info_mq` | ❌ read into its input model, dropped at the projection |
| `FractionInformativeReads` | 474,589 | ✅ `info_fraction_informative_reads` | ❌ same |
| `DB` (dbSNP membership, Flag) | 421,093 | ❌ no column | ❌ |
| `AQ` (systematic noise) | 84,331 | ❌ no column | ❌ same |
| `hotspot` (Flag) | 28 | ⚠️ column exists, wrong key read (`occurrence.py:179`) | ✅ **yes** — but on the *variant*, not the occurrence |
| `FILTER` (record field) | all | ✅ `filter` | ✅ |
| `MQRankSum`, `ReadPosRankSum` | declared, **never emitted** | ✅ columns exist | ❌ |

So of the INFO surface, **`hotspot` is the only field QLIN keeps that we don't** — and only
because it routes it to the variant. Everything else we either already store or neither
pipeline stores.

**23 of the 26 `info_*` columns will be NULL for every TO row**: `info_baseq_rank_sum`,
`info_excess_het`, `info_fs`, `info_ds`, `info_inbreed_coeff`, `info_mleac`, `info_mleaf`,
`info_mq0`, `info_m_qrank_sum`, `info_qd`, `info_r2_5p_bias`, `info_read_pos_rank_sum`,
`info_sor`, `info_vqslod`, `info_culprit`, `info_haplotype_score`, `info_hotspotallele`,
`info_old_record`, `info_germq`, `info_tlod`, `info_mapq`. This is the main cost of the
single-table decision (§5.3): the dbt `should_not_contain_only_null` exclusion list has to be
widened to cover them.

**`QUAL` is `.` on all 474,589 records** → `record.QUAL is None` → `quality` is always
NULL. (Consistent with the existing `# Constant by construction` exclusion for `quality`
in `somatic_snv_occurrence.yml`.)

**FORMAT fields**: `GT:SQ:AD:AF:F1R2:F2R1:DP:SB:MB` (+`:PS` on 72,133 records).

| FORMAT field | In TO VCF | Radiant column | QLIN persists |
|---|---|---|---|
| `SQ` — somatic quality (mean 22.2, max 82.0) | ✅ | ❌ | ✅ **yes** |
| `AD` — allele depths | ✅ | ✅ | ✅ |
| `DP` — depth | ✅ | ✅ | ✅ |
| `GT` — calls | ✅ | ✅ | ✅ |
| `AF` — allele fraction | ✅ | ✅ | ❌ — derives its ratio from `AD` instead |
| `F1R2` / `F2R1` — read-orientation counts (FFPE / oxidation artefact detection) | ✅ | ❌ | ❌ |
| `SB` / `MB` — strand-bias / mate-bias components | ✅ | ❌ | ❌ |
| `PS` — phase set id | ✅ | ❌ (`tumor_phased` is a bool only) | ❌ |
| phased flag | ✅ | ✅ | ❌ |
| `GQ` | **not emitted** — `SQ` is the somatic analogue | — | — |

**`SQ` is the only FORMAT field QLIN keeps that we don't**, which resolves OQ-3 in the
affirmative: a `tumor_sq` column is required for functional parity (§7.3). For
`F1R2`/`F2R1`/`SB`/`MB`/`PS`, neither pipeline stores them — capturing those would be a fresh
decision about wanting artefact-detection signal, not a parity gap. Radiant is ahead on one
field: we store `tumor_phased` / `normal_phased`; QLIN does not keep the phased flag for
somatic.

### 4.6 CSQ contract

```
Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|EXON|INTRON|HGVSc|HGVSp|
cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|
STRAND|FLAGS|PICK|VARIANT_CLASS|SYMBOL_SOURCE|HGNC_ID|CANONICAL|RefSeq|REFSEQ_MATCH|SOURCE|
REFSEQ_OFFSET|HGVS_OFFSET|HGVSg|CLIN_SIG|SOMATIC|PHENO|PUBMED
```

All 36 fields, same three-way comparison:

| CSQ field | In TO VCF | Radiant column | QLIN persists |
|---|---|---|---|
| `Consequence` | ✅ | ✅ `consequences` | ✅ |
| `IMPACT` | ✅ | ✅ `vep_impact` + derived `impact_score` | ✅ |
| `SYMBOL` | ✅ | ✅ `symbol` | ✅ |
| `Feature` | ✅ | ✅ `transcript_id` | ✅ |
| `BIOTYPE` | ✅ | ✅ `biotype` | ✅ |
| `STRAND` | ✅ | ✅ `strand` | ✅ |
| `EXON` | ✅ | ✅ `exon.rank` / `exon.total` | ✅ |
| `HGVSc` | ✅ | ✅ `hgvsc` + derived `dna_change` | ✅ |
| `HGVSp` | ✅ | ✅ `hgvsp` + derived `aa_change` | ✅ |
| `HGVSg` | ✅ | ✅ `hgvsg` | ✅ |
| `VARIANT_CLASS` | ✅ | ✅ `variant_class` | ✅ |
| `CANONICAL` | ✅ | ✅ `is_canonical` | ✅ |
| `PICK` | ✅ | ✅ `is_picked` | ❌ — derives "picked" itself, per locus |
| `SOURCE` | ✅ | ⚠️ code looks up `"Source"`, so `source` is NULL | ❌ |
| `Gene` | ✅ | ❌ no column | ✅ `ensembl_gene_id` |
| `Feature_type` | ✅ | ❌ | ✅ |
| `INTRON` | ✅ | ❌ | ✅ |
| `cDNA_position` | ✅ | ❌ | ✅ |
| `CDS_position` | ✅ | ❌ | ✅ |
| `Protein_position` | ✅ | ❌ | ✅ |
| `Amino_acids` | ✅ | ❌ | ✅ |
| `Codons` | ✅ | ❌ | ✅ |
| `RefSeq` | ✅ | ❌ | ✅ `refseq_mrna_id` |
| `PUBMED` | ✅ | ❌ | ✅ — on the variant |
| `Allele` | ✅ | ❌ | ❌ |
| `Existing_variation` | ✅ | ❌ | ❌ |
| `DISTANCE`, `FLAGS` | ✅ | ❌ | ❌ |
| `SYMBOL_SOURCE`, `HGNC_ID` | ✅ | ❌ | ❌ |
| `REFSEQ_MATCH`, `REFSEQ_OFFSET`, `HGVS_OFFSET` | ✅ | ❌ | ❌ |
| `CLIN_SIG`, `SOMATIC`, `PHENO` | ✅ | ❌ | ❌ |
| MANE | **absent from this CSQ** | ⚠️ looks up `ManeSelect` → NULL/False | ✅ — but from a separate Ensembl reference dataset, not the CSQ |

**The existing consequence parser works unmodified** — every field it requires is present.

Three observations:

- **Radiant keeps less positional/annotation detail than QLIN**: `Gene`, `Feature_type`,
  `INTRON`, the three `*_position` fields, `Amino_acids`, `Codons` and `RefSeq` are all
  persisted by QLIN and dropped by us. None are needed for TO specifically; noted as
  pre-existing scope difference, not a TO gap.
- **Two Radiant fields are silently never populated**, both pre-existing and both also
  affecting germline: `source` (case mismatch — `"Source"` vs `SOURCE`, a one-word fix) and
  `mane_select` / `is_mane_select` / `is_mane_plus`. Note QLIN does **not** get MANE from the
  CSQ either — it joins an Ensembl mapping reference dataset — so this is not fixable by
  reading a CSQ field. The TN VCF (VEP v111) *does* carry `MANE`/`MANE_SELECT`/
  `MANE_PLUS_CLINICAL`, so there the data is present and unread.
- **Radiant reads `PICK` where QLIN computes its own pick** per locus. Ours is cheaper and
  follows VEP's own ordering; theirs can apply gene-level preferences. Different approaches,
  neither a gap.

---

## 5. Storage Decision — SETTLED

> **DECIDED: one table, no discriminator column.** TO rows go into the existing
> `somatic__snv__occurrence` with the whole `normal_*` block left NULL. TO/TN is **not
> stored on the occurrence row** — it is a property of the task, inferred where it is
> already inferred today: in the frequency SQL. **A somatic task is tumor-only iff it has
> exactly one tumoral aliquot and no normal aliquot.** Chosen for ETL and API simplicity.

### 5.1 What this means

The occurrence table is **completely unchanged** by this decision — no new column, no
generated column, no backfill, no `ALTER`. Likewise unchanged: the Iceberg schema, the
partition-swap operator, `somatic_snv_occurrence_copy_partition.sql` (its `SELECT *` stays
valid), `somatic_snv_occurrence_insert_partition_delta.sql`, and
`snv_variant_part_insert_part.sql`. `normal_seq_id` and every `normal_*` column are already
nullable (`init/somatic_snv_occurrence_create_table.sql:6, 46-57`), so the table holds a TO
row as-is.

```
somatic__snv__occurrence           ← unchanged by this decision
  part, task_id, tumor_seq_id, locus_id     DUPLICATE KEY
  normal_seq_id    INT NULL                 NULL for TO
  tumor_*   …                               always populated
  normal_*  …                               NULL for TO

TO/TN resolved via task_id ──────────────────────┐
                                                 ▼
staging_sequencing_experiment
  task_id, aliquot, patient_id, experimental_strategy, histology_type
  ⇒ one 'tumoral' aliquot AND zero 'normal' aliquots  ⇔  tumor-only
```

### 5.2 The rule — extending an inference that already exists

`somatic_snv_staging_variant_freq_insert.sql` **already infers the analysis type** today
(`lines 11-13`):

```sql
COUNT(DISTINCT CASE WHEN histology_type = 'tumoral' THEN seq_id END) > 0
AND
COUNT(DISTINCT CASE WHEN histology_type = 'normal'  THEN seq_id END) > 0 AS is_tumor_normal
```

So TO support does not introduce a new mechanism — the predicate is the **mirror image** of
the one already there, evaluated at a different grain:

| | Today | Proposed |
|---|---|---|
| Grain | `GROUP BY case_id, part, patient_id, experimental_strategy` | `GROUP BY task_id, part, patient_id, experimental_strategy` |
| Predicate | one or more `tumoral` **and** one or more `normal` ⇒ tumor-normal | exactly one `tumoral` **and** zero `normal` ⇒ tumor-only |

```sql
somatic_tasks AS (
    SELECT
        task_id,
        part,
        patient_id,
        experimental_strategy,
        COUNT(DISTINCT CASE WHEN histology_type = 'tumoral' THEN aliquot END) = 1
        AND
        COUNT(DISTINCT CASE WHEN histology_type = 'normal'  THEN aliquot END) = 0
            AS is_tumor_only
    FROM {{ mapping.starrocks_staging_sequencing_experiment }}
    WHERE analysis_type = 'somatic'
      AND tenant_code = %(tenant_code)s
      AND part = %(part)s
    GROUP BY task_id, part, patient_id, experimental_strategy
),
```

The same rule applies at ingestion (§7.1).

> **Both halves of the conjunction are load-bearing.** `COUNT(tumoral) = 1` alone is *also*
> true of a tumor-normal task, so it would classify every TN analysis as TO. The
> `COUNT(normal) = 0` clause is what makes the two mutually exclusive.

Why this predicate:

- **Task-scoped**, which §6.1 requires. Grouped by `case_id`, a case holding both a TO task
  and a TN task has both a tumoral and a normal sample, so `is_tumor_normal` evaluates to
  `true` and the TO occurrences are absorbed into the TN cohort. Grouping by `task_id`
  separates them.
- **No clinical-model change.** `task_id`, `aliquot` and `histology_type` are all already on
  `staging_sequencing_experiment` (`init/staging_sequencing_experiment_create_table.sql:4,
  20`; `PRIMARY KEY (case_id, seq_id, task_id)`).
- **Requiring the sample to be `tumoral` is deliberate.** A bare "exactly one aliquot" rule
  would ingest a malformed task containing *only a normal* sample as tumor-only, writing the
  normal's depths into the `tumor_*` columns and adding a phantom patient to the `pn_to_*`
  denominator. Naming the histology rejects that.
- **No new dependency on `histology_type`.** The somatic path already requires it to locate
  the tumor index for TN (`process.py:141-144`), so this leans on metadata the pipeline
  cannot function without regardless.
- **Nothing stored, nothing to drift.** No second copy of the truth to reconcile with
  `normal_seq_id`.

### 5.3 Accepted trade-offs

Recorded for completeness; none were judged blocking.

- **Consumers must join to the task to split TO from TN.** The frequency SQL already joins
  `staging_sequencing_experiment` for the patient/strategy breakdown, so it costs nothing
  there. A future API needing the split pays one join — accepted as the price of leaving
  the occurrence table untouched.
- **`normal_seq_id IS NULL` remains available as a shortcut** on the occurrence row and
  will agree with the task rule in practice. It is a convenience, not the definition.
- A TO row physically stores ~11 always-NULL `normal_*` columns, plus (per §4.5) 23 NULL
  `info_*` columns, a NULL `quality`, and possibly a NULL `tumor_zygosity`. StarRocks
  compresses NULLs well.
- The dbt `should_not_contain_only_null` exclusion list becomes the union across TO and TN
  rather than being meaningful per analysis (§10).
- Robustness depends on task composition being correct upstream: a TN task whose normal
  experiment failed to register would have one tumoral aliquot and read as TO. §7.1 adds
  validations to catch that at ingestion rather than in the frequency denominator.
- Shape-wise this still matches QLIN, which keeps one dataset and splits on a per-analysis
  attribute (`bioinfo_analysis_code`, `enriched/Variants.scala:246-268`) — the difference
  being that Radiant derives the attribute instead of storing it.

---

## 6. Distinguishing Tumor-Only from Tumor-Normal

### 6.1 Constraint: detection must be per-TASK, not per-CASE

Confirmed requirement: **the same tumor sample can have both a TO and a TN analysis.**
This mirrors QLIN, where `TEBA` and `TNEBA` share a `service_request_id`
(`clin-variant-etl` `fhir/EnrichedClinical.scala:61`) and one aliquot can carry
`all_analyses = ['TO', 'TN']` (`enriched/SNVSomatic.scala:57-58`).

This invalidates any case-level derivation, and it means **the current TN frequency query
is already structurally wrong for a mixed cohort.** `somatic_snv_staging_variant_freq_insert.sql:3-23`
infers pairing from the case's sample composition:

```sql
somatic_sequencings AS (
    SELECT case_id, part, patient_id, experimental_strategy,
        ...
        COUNT(DISTINCT CASE WHEN histology_type = 'tumoral' THEN seq_id END) > 0
        AND
        COUNT(DISTINCT CASE WHEN histology_type = 'normal'  THEN seq_id END) > 0 AS is_tumor_normal
    FROM {{ mapping.starrocks_staging_sequencing_experiment }}
    WHERE analysis_type = 'somatic' ...
    GROUP BY case_id, part, patient_id, experimental_strategy
),
```

A case containing a TO task **and** a TN task has both a `tumoral` and a `normal` sample,
so it evaluates `is_tumor_normal = true` — and the TO occurrences get silently counted in
the TN cohort.

Compounding this, the carrier CTE joins on `seq_id`:

```sql
JOIN {{ mapping.starrocks_staging_sequencing_experiment }} s ON s.seq_id = o.tumor_seq_id
```

With one tumor sample analysed both ways, that join produces **duplicate rows** for the
same locus/patient. `COUNT(DISTINCT patient_id)` masks the duplication for `pc`, but the
grouping is no longer meaningful.

**Both are fixed as part of this ticket** (decided; OQ-6 closed) — the join moves to
`task_id`, and the TO/TN split comes from the task rather than the case. See §8.1.

Two consequences of folding it in rather than splitting it out:

- SJRA-1751 **changes behaviour for existing tumor-normal data**, not just for new tumor-only
  data. The TN numbers it produces will differ from today's for any mixed cohort.
- It needs the mixed-cohort regression test called out in §10 — a case carrying both a TO and
  a TN analysis on the same tumor sample — which is the scenario the current query gets wrong.

### 6.2 Where the TO signal comes from — SETTLED

> **DECIDED: a somatic task is tumor-only iff it has exactly one `tumoral` aliquot and no
> `normal` aliquot.** No clinical
> model change, no new `case_type_code`, no new `task_type_code`, no column on the
> occurrence row. See §5.2.

```python
# ingestion — radiant/tasks/vcf/snv/somatic/process.py
is_tumor_only = (
    len(filtered_experiments) == 1
    and filtered_experiments[0].histology_type == "tumoral"
)
```

```sql
-- frequency SQL — one row per task
COUNT(DISTINCT CASE WHEN histology_type = 'tumoral' THEN aliquot END) = 1
AND
COUNT(DISTINCT CASE WHEN histology_type = 'normal'  THEN aliquot END) = 0
    AS is_tumor_only
```

The alternatives considered and rejected:

| Option | Why rejected |
|---|---|
| New `case_type_code` (e.g. `'somatic_tumor_only'`) | It is a **case**-level attribute, so it cannot describe a case holding both a TO and a TN task — it contradicts §6.1. It would also require a clinical-schema change plus seeds, and would invalidate every `analysis_type = 'somatic'` predicate in the codebase (`somatic_snv_staging_variant_freq_insert.sql:15`). |
| New `task_type_code` (e.g. `radiant_somatic_tumor_only_annotation`) | Correct and explicit — the closest analogue to QLIN's `bioinfo_analysis_code` — but it requires a new task type, a change to the `WHERE` clause in `init/staging_external_sequencing_experiment_create_table.sql:66`, and the upstream task producer to emit it. Not worth the coordination cost when the aliquot count already carries the information. |
| Bare "exactly one aliquot", without naming the histology | Simpler, and independent of `histology_type` — but it would ingest a malformed task containing *only a normal* sample as tumor-only, putting the normal's depths in the `tumor_*` columns and a phantom patient in the `pn_to_*` denominator. Naming the histology costs nothing, since the somatic path already depends on it to locate the tumor index for TN. |

The residual weakness of the chosen rule is that it cannot distinguish a genuine TO task
from a TN task whose normal experiment failed to register upstream. §7.1 adds ingestion
validations so this surfaces as an error rather than as a silent TO classification.

---

## 7. Ingestion Changes (VCF → Iceberg)

### 7.1 `radiant/tasks/vcf/snv/somatic/process.py`

**`get_somatic_indexes`** — allow a missing normal:

```python
def get_somatic_indexes(experiments: list[Experiment], samples: list[str]):
    # §5.2: exactly one 'tumoral' aliquot and no 'normal' ⇒ tumor-only
    if len(experiments) == 1:
        exp = experiments[0]
        if exp.histology_type != "tumoral":
            raise ValueError(
                f"Single-aliquot somatic task has histology_type "
                f"'{exp.histology_type}', expected 'tumoral': {exp}"
            )
        return samples.index(exp.aliquot), None

    tumor_index = None
    normal_index = None
    for exp in experiments:
        if exp.histology_type == "tumoral":
            tumor_index = samples.index(exp.aliquot)
        elif exp.histology_type == "normal":
            normal_index = samples.index(exp.aliquot)
    if tumor_index is None or normal_index is None:
        raise ValueError(
            f"Could not find both tumor and normal samples [{samples}] "
            f"in the VCF for the given experiments: {experiments}."
        )
    return tumor_index, normal_index
```

Note the shape. The single-aliquot branch is the **only** path that returns
`normal_index = None`, and it requires the sample to be `tumoral`; the multi-aliquot branch
keeps its original strictness and still raises if either index is unresolved. Two failure
modes are rejected rather than silently reinterpreted:

| Malformed input | Outcome |
|---|---|
| Two aliquots, normal missing from the VCF | raises (multi-aliquot branch) — **not** read as TO |
| One aliquot, histology `normal` or unset | raises — **not** read as TO with the normal's depths in `tumor_*` |

This is deliberately narrower than "normal not found ⇒ tumor-only", which would admit both.

**`get_sorted_task_experiments`** — the sort key must tolerate a missing normal:

```python
sort_key = {"tumoral": tumor_index}
if normal_index is not None:
    sort_key["normal"] = normal_index
```

**New validations.** The chosen rule cannot, on its own, tell a genuine TO task from a TN
task whose normal experiment failed to register upstream (§6.2). Two cheap checks make that
failure loud instead of silent — without them, the phantom TO task would be counted in the
`pn_to_*` denominator and quietly bias every TO frequency:

```python
# 1. a single-aliquot task's VCF must contain exactly one sample
if normal_index is None and len(vcf.samples) != 1:
    raise ValueError(
        f"Task {task.task_id} has one aliquot (tumor-only) but its VCF has "
        f"{len(vcf.samples)} samples: {vcf.samples} — likely a TN task with a "
        f"missing normal experiment."
    )

# 2. a somatic task should never carry more than two aliquots
if len(filtered_experiments) > 2:
    raise ValueError(
        f"Task {task.task_id} has {len(filtered_experiments)} aliquots; "
        f"somatic tasks support 1 (tumor-only) or 2 (tumor-normal)."
    )
```

Check 1 is the important one: it catches exactly the case the aliquot rule is blind to.

**Table target** — unchanged (`{namespace}.somatic_snv_occurrence`), per the §5 decision.

`RadiantSomaticAnnotationTask.gather_additional_args` (`experiment.py:88-96`) already
selects the row with `histology_type == "tumoral"` and needs **no change** for TO.

### 7.2 `radiant/tasks/vcf/snv/somatic/occurrence.py`

`process_occurrence` currently dereferences `normal_index` unconditionally
(`occurrence.py:187, 202-211`). `record.format("DP")[None]` and
`record.gt_ref_depths[None]` both raise.

```python
def process_occurrence(
    record: Variant,
    experiments: list[Experiment],
    common: Common,
    tumor_index: int,
    normal_index: int | None,
) -> dict:
    ...
    tumor_exp = experiments[tumor_index]

    # Tumor FORMAT — unchanged
    ...

    # Normal FORMAT — absent for tumor-only
    if normal_index is None:
        normal_exp = None
        n_dp = n_ad_ref = n_ad_alt = n_calls = n_has_alt = None
        n_ad_total = n_ad_ratio = n_af = n_zygosity = n_phased = None
    else:
        ...existing logic unchanged...
```

and in the emitted dict:

```python
"normal_seq_id": normal_exp.seq_id if normal_exp else None,   # NULL ⇒ tumor-only
```

Iceberg schema: `normal_seq_id` is already `required=False` (`occurrence.py:54`), as are
all `normal_*` fields — **no Iceberg schema change is needed for the normal block, and no
discriminator field is added**. Per §5 the TO/TN distinction is derived downstream from
`normal_seq_id`, so nothing in the Iceberg layer changes for it. The only new Iceberg
fields are the ones §7.3 requires (`tumor_sq`, `normal_sq`, `info_aq`).

### 7.3 New columns required by the real VCF

Characterisation (§4.5) turned up three concrete schema gaps.

#### `tumor_sq` — required for QLIN parity

`FORMAT/SQ` (somatic quality) is present on every record and is the field QLIN treats as
*the* somatic quality score (`normalized/SNVSomatic.scala:115, 135`). Radiant has no
equivalent, and because DRAGEN somatic emits **no `GQ`**, there is currently no
quality-per-call column at all for TO.

```python
NestedField(553, "tumor_sq", FloatType(), required=False),   # + normal_sq for TN symmetry
```

```sql
tumor_sq FLOAT,
```

`SQ` is `Number=A` (per-alt), so with split multi-allelics it is read as
`record.format("SQ")[tumor_index][0]`.

> This is the one item in this document that is **required** rather than optional: without
> it there is no way to express the somatic confidence of a TO call, and the QLIN parity
> requirement is not met.

#### `info_aq` — systematic noise score

`AQ` appears on 84,331 records and is the output of `--vc-systematic-noise`. It is the
signal DRAGEN uses to raise the `systematic_noise` filter and is useful for downstream
artefact triage.

#### `info_hotspotallele` — key mismatch, and a lead on SJRA-1559

`occurrence.py:179` reads INFO key `HotspotAllele` into an `INT` column. This VCF declares:

```
##INFO=<ID=hotspot,Number=0,Type=Flag,Description="Known somatic site, used to increase confidence in call">
```

Lowercase, and present on 28 records — so `info_hotspotallele` is NULL on every row.

**Decision: treat hotspot as a boolean and read the `hotspot` key into the existing
`info_hotspotallele` column.** The flag carries no allele index — it is presence/absence — so
the existing INT column holds 1/NULL and no new column, type change or migration is needed.
The `...Allele` suffix becomes mildly inaccurate; renaming costs more than the cosmetic gain.

Read *both* keys, since `HotspotAllele` is not necessarily wrong — it may match the caller the
schema was originally modelled on, and simply does not match these DRAGEN files.

QLIN also treats hotspot as a boolean, sourced from the same lowercase key and defaulting to
null when the field is absent — so boolean is the parity-consistent choice. One structural
difference, for the record: QLIN carries hotspot on the **variant**, rolled up across
occurrences as "a hotspot if any sample's VCF said so", whereas Radiant's column is
per-occurrence. Equivalent for ETL; it would only matter if the portal wanted to filter
*variants* by hotspot, which is an API concern and out of scope here. (Not to be confused with
QLIN's separate `cancer_hotspots` public annotation dataset, which is unrelated to this flag.)

> **Lead on SJRA-1559.** `info_hotspotallele` already sits in the dbt
> `should_not_contain_only_null` exclusion list under
> `# Probably same as SJRA-1559 (waiting for investigation)`, alongside 20 other `info_*`
> columns. §4.5 offers a likely explanation for that whole cluster: the schema carries
> GATK/Mutect2-era fields (`FS`, `QD`, `SOR`, `VQSLod`, `MQRankSum`, `ExcessHet`,
> `InbreedCoeff`, `MLEAC`/`MLEAF`, `HaplotypeScore`…) that DRAGEN does not emit at all.
> **Confirmed only for the tumor-only file** — a TN VCF would be needed to establish whether
> the same explanation covers the TN rows. Worth passing to whoever owns SJRA-1559.

`DB` (dbSNP membership, 421,093 records) is also uncaptured, but Radiant resolves
rsIDs elsewhere, so this is optional. `F1R2`/`F2R1`/`SB`/`MB`/`PS` are left out — they are
diagnostic-level detail with no current consumer.

### 7.4 Zygosity for TO — revised

`adjust_somatic_calls_and_zygosity` (`occurrence.py:284-331`) is **TO-safe as code** — it
is applied per sample and never references the normal, so it runs unmodified.

But §4.4 shows the *output* is uninformative. DRAGEN somatic emits `0/1` on 99.9 % of
records regardless of true zygosity, so:

- `tumor_zygosity` will be `HET` for ~100 % of TO rows and `HOM` for none;
- in particular the **98,049 germline-homozygous calls (AF ≥ 0.60) are labelled `HET`**,
  which is affirmatively wrong rather than merely uninformative.

#### What QLIN does — the same thing, unmitigated

Worth stating plainly, because it sets the parity bar.

QLIN derives zygosity purely from the genotype calls — a lookup mapping the call pair to
`HOM`/`HET`/`HEM`/`WT`/`UNK`, with no input from allele fraction or depth
(`datalake-lib`, `implicits/GenomicImplicits.scala:721-732`). Because DRAGEN somatic emits
`0/1`, every tumor-only record lands on the `HET` branch.

The somatic path applies it in three steps (`normalized/SNVSomatic.scala`): compute a
provisional zygosity from the raw calls, use it to blank out calls with insufficient alt
support, then recompute zygosity from the adjusted calls and drop whatever no longer has an
alt. Structurally identical to Radiant's `adjust_somatic_calls_and_zygosity` — the support
threshold is 3 rather than 2, and QLIN drops the no-alt rows one layer earlier than Radiant,
which drops them at the StarRocks insert
(`somatic_snv_occurrence_insert_partition_delta.sql:61`). Net behaviour is equivalent.

**Critically, the somatic transform is shared by tumor-only and tumor-normal** — the analysis
code is only a parameter, and nothing in the zygosity path branches on it. So QLIN records
`HET` for essentially every tumor-only row, germline-homozygous calls included. Its QA
dictionary expects only `HOM`, `HEM`, `HET` (`qc/dictionary/package.scala:554-558`), of which
tumor-only contributes just `HET`.

#### Options

| | Approach | QLIN parity? | Assessment |
|---|---|---|---|
| **A** | Leave as-is — `HET` everywhere | ✅ **exact parity** | Zero work. Ships a column that is wrong for the ~98 K germline-homozygous PASS rows per sample. Acceptable if no consumer reads `tumor_zygosity` for TO. |
| **B** | Write `NULL` for TO rows | ❌ diverges | Honest — the column is nullable already. Costs a widened dbt `should_not_contain_only_null` exclusion. An improvement on QLIN, not parity with it. |
| **C** | Derive from VAF (e.g. `AF ≥ 0.85 → HOM`) | ❌ diverges | Rejected. In a tumor sample VAF reflects purity and copy number as much as zygosity, so any threshold is unsound — and *more* confidently wrong than A. |

**This is a team decision, not a technical one (OQ-7).** The trade-off is explicit: option
A is exactly what QLIN ships today and satisfies the parity requirement literally; option
B produces better data but is a deliberate divergence from it.

If forced to choose, I would take **B** — a NULL that says "unknown" is safer than a `HET`
that asserts something false about ~57 % of PASS rows, and `tumor_af` remains available
and truthful for anyone who needs the signal. But the parity requirement legitimately
points at A, so this should be an explicit choice rather than an implementation detail.

> **Interaction with dbt QA**: `somatic_snv_occurrence__validate_no_wt_tumor_zygosity`
> asserts no `WT` tumor zygosity. Under A and B alike the assertion still passes (TO rows
> are `HET` or NULL, never `WT`) — but confirm the test's NULL handling under B. Under
> the single-table decision (§5) the test is shared with TN, so if its intent is
> TN-specific it must be scoped — e.g. `WHERE normal_seq_id IS NOT NULL`.

### 7.5 Shared variant / consequence catalog

**Decision (confirmed): unchanged from TN — every biallelic record on a supported
chromosome writes to `snv_variant` and `snv_consequence`.** No TO-specific filtering.

This is consistent with germline and TN, which likewise apply no `FILTER` gate at Iceberg
write time (`germline/process.py:75-84`, `somatic/process.py:78-93`); the only gate is
`COALESCE(o.tumor_has_alt, FALSE)` at the Iceberg→StarRocks step.

Sharing these tables is unambiguously correct, and it is worth being precise about why —
the per-sample row counts in §4.1 look alarming but mostly do not translate into growth.

**The StarRocks side deduplicates by locus.** All four shared tables are PRIMARY KEY
tables, which in StarRocks means `INSERT INTO` upserts rather than appends:

| Table | Key |
|---|---|
| `snv__tmp_variant` | `PRIMARY KEY(locus_id)` |
| `snv__staging_variant` | `PRIMARY KEY(locus_id)` |
| `snv__variant` | `PRIMARY KEY(locus_id)` |
| `snv__consequence` | `PRIMARY KEY(locus_id, symbol, transcript_id)` |

A consequence row is a pure function of locus + transcript, so the same locus arriving
from a TO sample produces identical rows to the germline one and collapses onto it. The
catalog therefore grows by **novel loci only**. Given §4.3 (98.8 % of TO PASS calls are
dbSNP-known), overlap with an existing germline cohort should be high and the marginal
cost of the *n*-th TO sample small.

Nor is TO novel in kind: TN somatic already contributes loci that no germline sample
carries, and germline itself applies no `FILTER` gate at Iceberg write time.

**Where the raw counts are real:** the Iceberg `snv_variant` / `snv_consequence` tables are
partitioned by `task_id` (`process.py:66`), so every task persists its own complete row
set — ~470 K + ~1.36 M parquet rows per TO WES task, not deduplicated. Germline works the
same way; TO is simply a heavier producer, roughly 4-5× a germline WES sample. The costs
are S3 storage (cheap, linear) and a larger per-run `INSERT ... WHERE task_id IN (…)`
(runtime, manageable).

> **Conclusion: this is a throughput and cost consideration, not a design problem.** No
> TO-specific filtering is warranted. The one thing still worth measuring before rollout is
> a TO **WGS** sample, where a naive exome→genome extrapolation suggests 30-50× the record
> count — enough to matter for per-run insert time even though the deduplicated catalog
> growth stays modest (OQ-5).

---

## 8. Frequencies

Radiant's frequency model is kept — no adoption of QLIN's. Three levels:

```
L1  somatic__snv__staging_variant_frequency_part   per (tenant, part)
L2  somatic__snv__variant_frequency                per tenant, summed over parts
L3  snv__variant / snv__variant_partitioned        pooled across ALL tenants
```

**Confirmed carrier filter for TO — identical to TN:**

```sql
WHERE o.filter = 'PASS'
  AND o.tumor_ad_alt > 2
```

This keeps TO and TN internally comparable. It is stricter than QLIN, whose
`somaticFrequencyFilter` is `ad_alt >= 2` with no `PASS` requirement
(`clin-variant-etl` `utils/FrequencyUtils.scala:11`).

Measured against the reference VCF (§4.2), the `tumor_ad_alt > 2` clause is **redundant
for DRAGEN** — all 170,599 `PASS` records already satisfy it, because DRAGEN's own
`too_few_supporting_reads` filter enforces the depth floor. Keep the clause anyway: it
costs nothing and protects against callers that do not.

> **Read the resulting numbers correctly.** Per §4.3, a TO cohort's `pf_to_*` largely
> measures **germline allele frequency**, not somatic recurrence — 94.8 % of PASS calls
> are at germline VAF and 98.8 % are dbSNP-known. A common SNP will show `pf_to_wxs ≈ 0.5`.
> That is the correct output of this design (and QLIN behaves identically), but it is a
> different quantity from `pf_tn_*`, where the matched normal has already subtracted the
> germline. **TO and TN frequencies are not directly comparable and should not be summed
> or averaged together.**

**Denominator semantics**: `pn_to_wgs` / `pn_to_wxs` = distinct patients in the tenant
having at least one **TO somatic task** of that strategy. Because TO and TN can coexist
(§6.1), a patient with both counts in **both** `pn_to_*` and `pn_tn_*`. This is intended
and matches QLIN, which derives its two `pn` values from a `groupBy("bioinfo_analysis_code")`
over the same occurrence set (`enriched/Variants.scala:251-255`).

### 8.1 Level 1 — `somatic_snv_staging_variant_freq_insert.sql` (rewrite)

This file changes the most. It must move from case-level to task-level grouping (§6.1),
join occurrences on `task_id` instead of `seq_id`, and populate the four TO columns that
are currently `0`.

```sql
INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ mapping.starrocks_somatic_snv_staging_variant_frequency }}
WITH
somatic_tasks AS (
    SELECT
        task_id,
        part,
        patient_id,
        experimental_strategy,
        -- §5.2: exactly one 'tumoral' and zero 'normal' ⇒ tumor-only
        COUNT(DISTINCT CASE WHEN histology_type = 'tumoral' THEN aliquot END) = 1
        AND
        COUNT(DISTINCT CASE WHEN histology_type = 'normal'  THEN aliquot END) = 0
            AS is_tumor_only
    FROM {{ mapping.starrocks_staging_sequencing_experiment }}
    WHERE analysis_type = 'somatic'
      AND tenant_code = %(tenant_code)s
      AND part = %(part)s
    GROUP BY task_id, part, patient_id, experimental_strategy
),
patients_total_count_cohort AS (
    SELECT
        part,
        COUNT(DISTINCT CASE WHEN NOT is_tumor_only AND experimental_strategy = 'wgs' THEN patient_id END) AS cnt_tn_wgs,
        COUNT(DISTINCT CASE WHEN NOT is_tumor_only AND experimental_strategy = 'wxs' THEN patient_id END) AS cnt_tn_wxs,
        COUNT(DISTINCT CASE WHEN     is_tumor_only AND experimental_strategy = 'wgs' THEN patient_id END) AS cnt_to_wgs,
        COUNT(DISTINCT CASE WHEN     is_tumor_only AND experimental_strategy = 'wxs' THEN patient_id END) AS cnt_to_wxs
    FROM somatic_tasks
    GROUP BY part
),
freqs_tumor AS (
    SELECT
        o.part,
        o.locus_id,
        COUNT(DISTINCT CASE WHEN NOT t.is_tumor_only AND t.experimental_strategy = 'wgs' THEN t.patient_id END) AS pc_tn_wgs,
        COUNT(DISTINCT CASE WHEN NOT t.is_tumor_only AND t.experimental_strategy = 'wxs' THEN t.patient_id END) AS pc_tn_wxs,
        COUNT(DISTINCT CASE WHEN     t.is_tumor_only AND t.experimental_strategy = 'wgs' THEN t.patient_id END) AS pc_to_wgs,
        COUNT(DISTINCT CASE WHEN     t.is_tumor_only AND t.experimental_strategy = 'wxs' THEN t.patient_id END) AS pc_to_wxs
    FROM {{ mapping.starrocks_somatic_snv_occurrence }} o
    JOIN somatic_tasks t ON t.task_id = o.task_id      -- task_id, NOT seq_id (see §6.1)
    WHERE o.part = %(part)s
      AND o.filter = 'PASS'
      AND o.tumor_ad_alt > 2
    GROUP BY o.locus_id, o.part
)
SELECT
    %(tenant_code)s AS tenant_code,
    part,
    locus_id,
    pc_tn_wgs,
    (SELECT cnt_tn_wgs FROM patients_total_count_cohort)                     AS pn_tn_wgs,
    pc_tn_wgs / NULLIF((SELECT cnt_tn_wgs FROM patients_total_count_cohort), 0) AS pf_tn_wgs,
    pc_tn_wxs,
    (SELECT cnt_tn_wxs FROM patients_total_count_cohort)                     AS pn_tn_wxs,
    pc_tn_wxs / NULLIF((SELECT cnt_tn_wxs FROM patients_total_count_cohort), 0) AS pf_tn_wxs,
    pc_to_wgs,
    (SELECT cnt_to_wgs FROM patients_total_count_cohort)                     AS pn_to_wgs,
    pc_to_wgs / NULLIF((SELECT cnt_to_wgs FROM patients_total_count_cohort), 0) AS pf_to_wgs,
    pc_to_wxs,
    (SELECT cnt_to_wxs FROM patients_total_count_cohort)                     AS pn_to_wxs,
    pc_to_wxs / NULLIF((SELECT cnt_to_wxs FROM patients_total_count_cohort), 0) AS pf_to_wxs
FROM freqs_tumor
```

Two structural points about this query, both consequences of §5:

- **`freqs_tumor` joins `somatic_tasks` on `task_id`, not `seq_id`.** The current version
  joins `s.seq_id = o.tumor_seq_id` (`line 40`). Since one tumor sample can be analysed both
  TO and TN (§6.1) and `staging_sequencing_experiment` is keyed
  `(case_id, seq_id, task_id)`, that join yields one row per task using the sample and
  duplicates the occurrence. `COUNT(DISTINCT patient_id)` happens to absorb it for `pc`,
  but the grain is wrong and the TO/TN attribution is impossible. Joining on `task_id`
  fixes both.
- **The denominator must come from `somatic_tasks`, not from the occurrences.** A patient
  with a TO task but zero qualifying loci in this partition still belongs in `pn_to_*`.
  This is why the split cannot be read off the occurrence row alone, and why the task join
  is not merely a convenience.

No DDL change is required at this level — all twelve columns already exist (§2).

### 8.2 Level 2 — `somatic_snv_variant_frequency_insert.sql`

**No change required.** The rollup already sums `pc_to_*` and carries `pn_to_*` through
(`lines 7-8, 13-14, 24-25, 37-42`). It starts producing real numbers the moment level 1
does.

### 8.3 Level 3 — `snv_variant_insert.sql` and the variant tables

This is where new DDL is needed. `snv__variant` currently has **only** the TN columns
(`init/snv_variant_create_table.sql:5-6, 26-29`):

```sql
somatic_pf_tn_wgs DOUBLE,
somatic_pf_tn_wxs DOUBLE,
somatic_pc_tn_wgs INT(11),
somatic_pn_tn_wgs INT(11),
somatic_pc_tn_wxs INT(11),
somatic_pn_tn_wxs INT(11),
```

Six new columns are required, on **both** `snv__variant` and `snv__variant_partitioned`:

```sql
somatic_pf_to_wgs DOUBLE,
somatic_pf_to_wxs DOUBLE,
somatic_pc_to_wgs INT(11),
somatic_pn_to_wgs INT(11),
somatic_pc_to_wxs INT(11),
somatic_pn_to_wxs INT(11),
```

`snv_variant_insert.sql` extends its two somatic CTEs (`lines 36-53`) to carry the TO
columns through the cross-tenant pooling, exactly mirroring the TN treatment:

```sql
somatic_freq AS (
    SELECT locus_id,
           SUM(pc_tn_wgs) AS pc_tn_wgs, SUM(pc_tn_wxs) AS pc_tn_wxs,
           SUM(pc_to_wgs) AS pc_to_wgs, SUM(pc_to_wxs) AS pc_to_wxs
    FROM ( ... per-tenant UNION ALL ... )
    GROUP BY locus_id
),
somatic_pn AS (
    SELECT SUM(pn_tn_wgs) AS pn_tn_wgs, SUM(pn_tn_wxs) AS pn_tn_wxs,
           SUM(pn_to_wgs) AS pn_to_wgs, SUM(pn_to_wxs) AS pn_to_wxs
    FROM ( ... per-tenant ANY_VALUE ... )
)
```

`snv_variant_part_insert_part.sql` uses `SELECT %(variant_part)s AS part, v.*`, so it
picks up the new columns automatically, and its per-tenant `LEFT SEMI JOIN` (`lines 8-17`)
already reads `starrocks_somatic_snv_occurrence` — which now holds TO rows too. So it needs
**no change at all**, which is one of the concrete payoffs of the §5 single-table decision.

### 8.4 Migration SQL

Note what is *absent*: no `ALTER` on `somatic__snv__occurrence` for a discriminator, and no
backfill. Per §5 the TO/TN split is derived from the task at query time.

```sql
-- tumor-only frequency columns on the shared variant catalog
ALTER TABLE radiant.snv__variant ADD COLUMN somatic_pf_to_wgs DOUBLE  AFTER somatic_pf_tn_wxs;
ALTER TABLE radiant.snv__variant ADD COLUMN somatic_pf_to_wxs DOUBLE  AFTER somatic_pf_to_wgs;
ALTER TABLE radiant.snv__variant ADD COLUMN somatic_pc_to_wgs INT(11) AFTER somatic_pn_tn_wxs;
ALTER TABLE radiant.snv__variant ADD COLUMN somatic_pn_to_wgs INT(11) AFTER somatic_pc_to_wgs;
ALTER TABLE radiant.snv__variant ADD COLUMN somatic_pc_to_wxs INT(11) AFTER somatic_pn_to_wgs;
ALTER TABLE radiant.snv__variant ADD COLUMN somatic_pn_to_wxs INT(11) AFTER somatic_pc_to_wxs;

ALTER TABLE radiant.snv__variant_partitioned ADD COLUMN somatic_pf_to_wgs DOUBLE  AFTER somatic_pf_tn_wxs;
ALTER TABLE radiant.snv__variant_partitioned ADD COLUMN somatic_pf_to_wxs DOUBLE  AFTER somatic_pf_to_wgs;
ALTER TABLE radiant.snv__variant_partitioned ADD COLUMN somatic_pc_to_wgs INT(11) AFTER somatic_pn_tn_wxs;
ALTER TABLE radiant.snv__variant_partitioned ADD COLUMN somatic_pn_to_wgs INT(11) AFTER somatic_pc_to_wgs;
ALTER TABLE radiant.snv__variant_partitioned ADD COLUMN somatic_pc_to_wxs INT(11) AFTER somatic_pn_to_wgs;
ALTER TABLE radiant.snv__variant_partitioned ADD COLUMN somatic_pn_to_wxs INT(11) AFTER somatic_pc_to_wxs;
```

---

## 9. QLIN (`clin-variant-etl`) Comparison

Functional parity was requested. This table records where Radiant matches QLIN, where it
deliberately diverges, and where a gap remains.

| Concern | QLIN (`clin-variant-etl`) | Radiant TO (this design) | Verdict |
|---|---|---|---|
| TO/TN storage | One dataset `normalized_snv_somatic`, discriminated by a **stored** `bioinfo_analysis_code` (`TEBA`/`TNEBA`) | One table `somatic__snv__occurrence`, discriminator **derived** from the task's aliquot count | Same shape; Radiant derives rather than stores (§5) |
| TO/TN discriminator scope | Per-analysis (shares a `service_request_id`) | Must be per-task (§6.1) | Match |
| Same sample, both analyses | Supported — `all_analyses = ['TO','TN']` (`enriched/SNVSomatic.scala:57-58`) | Supported | Match |
| Carrier filter | `ad_alt >= 2`, no `PASS` (`FrequencyUtils.scala:11`) | `filter = 'PASS' AND tumor_ad_alt > 2` | **Deliberate divergence** — Radiant's model is kept |
| Frequency granularity | TO / TN only | TO / TN **× WGS / WXS** (4 buckets) | Radiant is finer |
| Frequency shape | Nested struct `{pc, pn, pf}` (`freq_rqdm_tumor_only`) | Flat columns `pc_to_wgs`, `pn_to_wgs`, `pf_to_wgs`, … | Equivalent |
| Multi-tenant pooling | N/A (single tenant) | pc summed across tenants, pn summed across tenants (§8.3) | Radiant-specific |
| Zygosity adjustment | `ad_alt < 3` → no-call (`SNVSomatic.scala:93-97`) | `ad_alt < 2` → `UNK` (`occurrence.py:324`) | Same structure, different threshold. Pre-existing, code unchanged |
| Zygosity **for TO** | `HET` on ~100 % of rows — `SNVSomatic` is shared by TEBA/TNEBA, no branch on analysis type (`GenomicImplicits.scala:721-732`) | Same today | **Shared defect.** Keeping it = literal parity; NULL = deliberate improvement. §7.4 / OQ-7 |
| `FORMAT/SQ` (somatic quality) | Read and stored (`SNVSomatic.scala:115, 135`) | **Not stored** | **Gap — confirmed present in the input (§4.5). `tumor_sq` is a required addition (§7.3).** |
| Germline-background handling in TO frequencies | None — TO frequencies are gated solely by `ad_alt >= 2` (`FrequencyUtils.scala:11`) | None | **Same problem in QLIN.** Its TO frequencies carry the germline background too, unmitigated. Not a parity gap — any handling would be new capability for both. See OQ-4 |
| `INFO/AQ` systematic noise | Not read | **Not stored** | Gap — §7.3 proposes `info_aq` |
| Hotspot flag | n/a | Reads `HotspotAllele`; VCF emits `hotspot` | **Defect** — column always NULL (§7.3) |
| TO-only variants in the germline catalog | RQDM frequencies blanked for variants seen only in somatic TO (`Variants.scala:235-241`) | N/A — Radiant's germline columns are computed from germline occurrences only, so they are naturally `0` for a TO-only locus | No action |
| TO × TN cross-join | Performed | **Out of scope** (explicit) | Excluded |
| CNV TO | `CNVSomaticTumorOnly.scala` | **Out of scope** | Excluded |

---

## 10. Testing

| Level | What to add |
|---|---|
| `tests/unit/vcf/somatic/test_somatic_snv_occurrences.py` | `process_occurrence` with `normal_index=None` → every `normal_*` is `None`, every `tumor_*` populated |
| `tests/unit/vcf/somatic/test_somatic_snv_process.py` | `get_somatic_indexes` returns `(idx, None)` for a single `tumoral` aliquot; **raises** for a single aliquot whose histology is `'normal'` or unset; **still raises for a 2-aliquot task missing its normal** (the narrower branch, §7.1); validation 1 fires for a 2-sample VCF on a 1-aliquot task; validation 2 fires for a 3-aliquot task |
| `tests/resources/` | `test_somatic_snv_tumor_only.vcf` — a small single-sample fixture cut from the reference VCF (§4). Must cover: a `PASS` record, a `too_few_supporting_reads` record, a phased record with `PS`, a record with `AQ`, a `hotspot` record, an `AF ≥ 0.6` (germline-hom) record, and a record on an unsupported contig |
| Integration | A partition containing a TO task, a TN task, and a TO+TN pair on the same tumor sample — asserting `pn_to_*` and `pn_tn_*` both count that patient, and that TO rows do not leak into `pc_tn_*` |
| dbt QA | Scope TN-specific assertions on the shared `somatic__snv__occurrence` source with `WHERE normal_seq_id IS NOT NULL`; widen the `should_not_contain_only_null` exclusion list for the columns §4.5 shows are always NULL in TO. Add an assertion that a TO row has **every** `normal_*` column NULL, not just `normal_seq_id` |

The regression test for §6.1 (TO+TN on one tumor sample) is the most valuable one here —
it is the case the current TN query gets wrong.

---

## 11. Open Questions

| # | Question | Owner | Blocking |
|---|---|---|---|
| OQ-1 | ~~One table or two?~~ **RESOLVED — one table** (§5), `normal_*` NULL for TO, no discriminator column | — | Closed |
| OQ-2 | ~~How is TO signalled upstream?~~ **RESOLVED — a somatic task is tumor-only iff it has exactly one `tumoral` aliquot and no `normal` aliquot** (§5.2, §6.2). No clinical-model change needed | — | Closed |
| OQ-3 | ~~Does the TO VCF carry `FORMAT/SQ`?~~ **RESOLVED — yes** (§4.5). Add `tumor_sq FLOAT` (+ `normal_sq` for symmetry). Also decide on `info_aq` and the `hotspot`/`HotspotAllele` mismatch (§7.3) | — | Additive, but `tumor_sq` is required for parity |
| OQ-4 | Given **98.8 % of PASS calls are dbSNP-known and 94.8 % sit at germline VAF** (§4.3), is any germline-background handling needed at ETL level, or is it entirely an API/UI concern? **Same problem exists in QLIN** — its TO frequencies are equally unfiltered — so this would be new capability, not catch-up | Team | No — API/UI out of scope, but the answer may change what ETL must persist |
| OQ-5 | **TO WGS throughput.** WES produces 1.36 M consequence rows/sample into the Iceberg task partitions; WGS is in scope and could be 30-50×. Catalog growth is bounded (locus-keyed upsert, §7.5) so this is about per-run insert time and S3 volume, not table size. Measure on a real TO WGS VCF | Bioinfo | Yes, before WGS rollout |
| OQ-6 | ~~Separate bug ticket for the `is_tumor_normal` / `seq_id`-join defect?~~ **RESOLVED — fixed in this ticket** (§6.1, §8.1). It sits in the same query the TO frequency work must change anyway. Note this means SJRA-1751 also changes behaviour for existing TN data, and needs the mixed-cohort regression test (§10) | — | Closed |
| OQ-7 | **`tumor_zygosity` for TO** (§7.4). **QLIN has the identical defect and does not mitigate it** — `SNVSomatic` is shared by TEBA/TNEBA and writes `HET` for every TO row. So: keep `HET` (literal QLIN parity, ships data that is wrong for ~98 K rows/sample) or write NULL (better data, deliberate divergence)? Deriving from VAF is rejected. | Team | No — but ships wrong data if unanswered |

---

## 12. Files to Create / Modify

Reflects the settled decisions: **one table, no discriminator column** (§5) and **TO = a
task with exactly one `tumoral` aliquot and no `normal`** (§6.2). Notably there is **no new
file to create** — TO
ships as modifications to eleven existing ones, plus a test fixture.

**Ingestion (Python)**

| Action | File | Change |
|---|---|---|
| Modify | `radiant/tasks/vcf/snv/somatic/process.py` | Single-aliquot branch in `get_somatic_indexes` returning `normal_index=None`; sort key tolerates a missing normal; the two validations of §7.1 |
| Modify | `radiant/tasks/vcf/snv/somatic/occurrence.py` | Guard the normal FORMAT block against `normal_index=None`; **add `tumor_sq` / `normal_sq` (`NestedField(553-554)`)**; add `info_aq`; fix the `hotspot` / `HotspotAllele` key mismatch; **pending OQ-7** — NULL `tumor_zygosity` for TO, or leave at `HET` for QLIN parity (§7.4) |

**Occurrence table (new fields from §7.3 only — nothing for the TO/TN split)**

| Action | File | Change |
|---|---|---|
| Modify | `radiant/dags/sql/radiant/init/somatic_snv_occurrence_create_table.sql` | **+`tumor_sq FLOAT`, `normal_sq FLOAT`, `info_aq FLOAT`** (§7.3) |
| Modify | `radiant/dags/sql/radiant/somatic_snv_occurrence_insert_partition_delta.sql` | +`tumor_sq`, `normal_sq`, `info_aq` in the projection |

**Frequencies**

| Action | File | Change |
|---|---|---|
| Modify | `radiant/dags/sql/radiant/somatic_snv_staging_variant_freq_insert.sql` | **The main change.** Regrain the existing `is_tumor_normal` CTE from `case_id` to `task_id`, invert the predicate to "one `tumoral`, zero `normal`", join occurrences on `task_id` instead of `seq_id`, populate the four TO columns (§8.1) |
| Modify | `radiant/dags/sql/radiant/snv_variant_insert.sql` | Carry `pc_to_*` / `pn_to_*` through the cross-tenant pooling (§8.3) |
| Modify | `radiant/dags/sql/radiant/init/snv_variant_create_table.sql` | +6 TO frequency columns |
| Modify | `radiant/dags/sql/radiant/init/snv_variant_partitioned_create_table.sql` | +6 TO frequency columns |

**Fixtures and QA**

| Action | File | Change |
|---|---|---|
| Create | `tests/resources/test_somatic_snv_tumor_only.vcf` | Single-sample fixture cut from the reference VCF (§10) |
| Modify | `radiant/dags/sql/clinical/seeds.sql` | A somatic case whose task has a single tumoral aliquot; ideally also a case carrying both a TO and a TN task on the same tumor sample, to cover §6.1 |
| Modify | `radiant/data_qa/sources/somatic_snv_occurrence.yml` | Scope TN-specific tests with `normal_seq_id IS NOT NULL`; widen the null-exclusion list per §4.5 |
| Modify | `tests/unit/vcf/somatic/test_somatic_snv_process.py`, `test_somatic_snv_occurrences.py` | Per §10 |

**Confirmed to need no change**

| File | Why |
|---|---|
| `somatic_snv_variant_frequency_insert.sql` | Already sums `pc_to_*` and carries `pn_to_*` (§8.2) |
| `somatic_snv_occurrence_copy_partition.sql` | `SELECT *` stays valid — no generated column was added (§5.1) |
| `snv_variant_part_insert_part.sql` | `v.*` picks up the new columns; already reads the somatic occurrence table (§8.3) |
| `radiant/tasks/vcf/experiment.py` | `RadiantSomaticAnnotationTask` covers TO unchanged; `gather_additional_args` already selects the `tumoral` row (§7.1) |
| `radiant/tasks/data/radiant_tables.py` | No new table, no new mapping key |
| `radiant/dags/import_part.py` | Same task type, same TaskGroup, same operators |
| `init/staging_external_sequencing_experiment_create_table.sql` | No new `task_type_code` or `case_type_code` (§6.2) |
| Iceberg schema, `normal_*` block | Already fully nullable (`occurrence.py:54`) |

---

## 13. Out of Scope

- **API and UI** — explicitly excluded.
- **CNV** (germline or somatic tumor-only) — separate effort.
- **TO × TN cross-join** — explicitly excluded; QLIN performs one, Radiant will not.
- **Adopting QLIN's frequency model** — Radiant's `pc`/`pn`/`pf` × strategy model is kept.
- **Exomiser** — not applicable to somatic.
- **Revisiting `adjust_somatic_calls_and_zygosity` thresholds** (§7.4) — pre-existing
  divergence from QLIN, applies equally to TN.
