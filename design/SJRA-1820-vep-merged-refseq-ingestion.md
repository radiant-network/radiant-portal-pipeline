# Ingesting VEP `--merged` VCFs: supporting RefSeq transcripts

**Status:** analysis / proposal — no code written yet

**Audience:** dev, QA, product owner

**Reference file used for all figures:** `variants.fam1.snv.vep.slivar.vcf.gz` (VEP v114, `--merged --mane`, GRCh38, one trio, 48,468 variants). All figures re-measured after the `--mane` rerun.

---

## 1. Why this document

Until now the pipeline has ingested VCFs annotated by VEP against a **single** transcript
set — Ensembl. Upstream now produces VCFs annotated with VEP's `--merged` option, which
annotates each variant against **both** Ensembl and RefSeq transcripts in one pass.

This document describes what changes in the data, what it means for the portal, and the
decisions we need before implementing. The goal is to:

1. Ingest merged VCFs without breaking the files we already support.
2. Keep RefSeq consequences alongside Ensembl consequences (not throw them away).
3. Record **which catalogue each consequence came from**, so everything downstream can tell an
   Ensembl annotation from a RefSeq one and label, link and filter accordingly.

---

## 2. What a merged VCF actually contains

VEP writes one annotation block per transcript, in a single `CSQ` field. In a merged file
those blocks come from two catalogues:

| | per file | per variant (avg) |
|---|---|---|
| Ensembl transcript annotations | 479,333 | ~9.9 |
| RefSeq transcript annotations | 470,518 | ~9.7 |
| Neither (intergenic — no transcript at all) | 11 | — |

**The headline number: consequence volume roughly doubles.** Everything downstream that is
sized per-consequence (Iceberg files, the `snv__consequence` table, the derived
`snv__consequence_filter` table, load duration) should be planned for ~2×.

The two catalogues largely describe the same biology with different identifiers — an
Ensembl `ENST…` transcript and a RefSeq `NM_…` transcript for the same gene will usually
agree on the consequence, and will usually disagree on the exact transcript version, the
HGVS notation, and sometimes the gene symbol.

---

## 3. How we tell Ensembl from RefSeq

Merged files carry an explicit `SOURCE` column in the annotation. Older, non-merged files
do not — so we need a rule that works for both.

**Proposed rule, in order:**

1. **Use `SOURCE` when the file provides it.** This is VEP's own answer; it is authoritative.
2. **Otherwise derive it from the transcript identifier.** The two catalogues use disjoint
   namespaces: Ensembl is `ENST…`, RefSeq is `NM_ / NR_ / XM_ / XR_…`.
3. **Otherwise derive it from the gene identifier** (Ensembl uses `ENSG…`, RefSeq uses a
   numeric NCBI gene id) — needed because some rows have no transcript at all.
4. **Otherwise leave it empty.** Intergenic annotations have neither a gene nor a
   transcript and belong to no catalogue. We record "unknown" rather than guessing.

Rules 2 and 3 are both kept because they fail on opposite rows: rule 2 is the only signal
for regulatory features (`ENSR…` feature, *empty* gene), and rule 3 is the only signal when
the feature id is empty or non-standard. Cost is one extra branch.

We verified on the reference file that rules 1–3 never contradict each other: every one of
the 949,851 annotation blocks classified identically by all three signals. So the fallback
is safe for the older files, and the rule is a single code path for every file type.

---

## 4. What changes, layer by layer

| Layer | Change | Impact |
|---|---|---|
| VCF extraction | Populate a `source` value on every consequence, per the rule above | Small, contained |
| Iceberg `snv_consequence` | Column already exists in the schema; it starts being filled | No schema change; ~2× rows for merged files |
| StarRocks `snv__consequence` | New `source` column, plus `mane_pair_transcript_id` and `scores_from_mane_pair` | `ALTER TABLE`, no reload — see §6 and §7 |
| StarRocks `snv__consequence_filter` | Ensembl as today, plus the RefSeq annotations Ensembl does not already provide | ~11% more rows instead of ~100% — see §7 |
| `snv__variant` (+ tmp, staging, partitioned) | New `pick_source` column: which catalogue VEP's picked consequence came from | Pick itself unchanged — see §5 |
| External annotation joins (dbNSFP, gnomAD constraint) | Keyed on Ensembl transcript ids; RefSeq MANE rows borrow their twin's scores | Covers 94.7% of variants — see §7 |

---

## 5. VEP's "pick" is not per source

Worth stating clearly, because it is the most common misunderstanding: with `--merged`,
VEP does **not** choose one best Ensembl transcript and one best RefSeq transcript. It runs
its selection once over the combined list and flags exactly **one** winner per variant.

On the reference file: 48,468 variants, 48,468 winners. The winner was a RefSeq transcript
for **681 variants (1.4%)**, and in 679 of those cases Ensembl transcripts were available
on the same variant and simply scored lower.

### The pick order used for this file

No `--pick_order` was passed, so VEP's default applies. It walks these criteria in order and
stops at the first one that separates the candidates:

`mane_select` → `mane_plus_clinical` → `canonical` → `appris` → `tsl` → `biotype` → `ccds` →
`rank` (consequence severity) → `length`

**Source is not one of them.** `ensembl` and `refseq` are valid `--pick_order` criteria, but
they are not in the default list. So nothing in the ranking prefers one catalogue over the
other. When candidates tie on all nine criteria the winner is simply the first block VEP
evaluated — and VEP emits every Ensembl block before any RefSeq block (verified on every
variant in the file). That ordering, not a rule, is why Ensembl wins 98.5% of the picks.

Where the 681 RefSeq winners come from:

| | count |
|---|---|
| RefSeq transcript is MANE Select, Ensembl candidates are not | 256 |
| RefSeq transcript is canonical, no MANE on either side | 423 |
| No Ensembl transcript exists on the variant at all | 2 |

### Two worked examples

**RefSeq wins on merit** — `chr3:72446626 C>G`, gene *RYBP*. The RefSeq transcript is MANE
Select, so criterion 1 decides it before `canonical` is ever consulted, even though both
Ensembl candidates are canonical:

| source | transcript | consequence | biotype | MANE | canonical | picked |
|---|---|---|---|---|---|---|
| Ensembl | `ENST00000477973` | upstream_gene_variant | protein_coding | — | YES | |
| Ensembl | `ENST00000810668` | downstream_gene_variant | lncRNA | — | YES | |
| **RefSeq** | **`NM_012234.7`** | upstream_gene_variant | protein_coding | **MANE_Select** | YES | **✓** |

**RefSeq is the only annotation** — `chr18:73774840 T>C`. One annotation block on the whole
variant, from a RefSeq-only lncRNA model. Ensembl has no transcript at this locus, so
dropping RefSeq would leave this variant with no gene annotation at all:

| source | transcript | consequence | biotype | MANE | canonical | picked |
|---|---|---|---|---|---|---|
| **RefSeq** | **`XR_935622.3`** | intron_variant & non_coding_transcript_variant | lncRNA | — | YES | **✓** |

Only 2 variants in this file are of the second kind, but they are the clearest illustration
of why RefSeq is not redundant: there is nothing to fall back on.

Consequence for the portal: if we do nothing, the headline transcript displayed for ~1.4%
of variants will silently switch from an `ENST…` to an `NM_…`, only for merged files.
Anything that assumes an Ensembl identifier there (transcript links, joins to the
per-tenant variant catalogue) will come up empty for those variants.

**Recommendation: keep VEP's pick exactly as it is, and record which source it came from.**

Overriding the pick to force an Ensembl transcript is the wrong fix, for a reason that is
easy to miss: the headline is not one field, it is a *block* of fields that all describe the
same transcript — `transcript_id`, `hgvsc`, `hgvsp`, `dna_change`, `aa_change`, `symbol`,
the MANE and canonical flags. They have to stay internally coherent.

Forcing Ensembl leaves only two ways to do it, and both are worse:

- **Re-pick a lower-ranked Ensembl transcript.** In the 681 cases the RefSeq transcript won
  on merit — it scored higher on VEP's own criteria, and the Ensembl candidates on the same
  variant were less relevant (non-coding, or a milder consequence). We would be displaying a
  deliberately worse annotation.
- **Take the Ensembl identifier but keep the rest.** That produces an `aa_change` and an
  `hgvsc` that do not belong to the displayed transcript. That is a data-integrity bug, not
  a display preference.

So: leave the pick alone, and add a **`pick_source`** column to `snv__variant` (and its
staging, tmp and partitioned equivalents) carrying the source of the picked consequence.
The portal then knows what it is showing and can link, label and format accordingly — an
`NM_…` transcript links to RefSeq, not to Ensembl.

The concern above is real, but the column is what solves it. Anything that assumes an
Ensembl identifier must read `pick_source` rather than assume.

**The HGVS identifiers make this concrete.** They are not source-neutral strings — each one
is prefixed with the accession it was computed against, and the two catalogues use different
namespaces:

| field | Ensembl | RefSeq | source-neutral? |
|---|---|---|---|
| `hgvsc` | `ENST00000831140.1:n.1889G>A` | `NM_…` / `NR_…` / `XM_…` / `XR_…` | no |
| `hgvsp` | `ENSP…:p.…` | `NP_…` / `XP_…` | no — and note this is a *protein* accession, a third namespace distinct from `transcript_id` |
| `hgvsg` | `chr1:g.14653C>T` | identical | **yes** |

`hgvsg` is genomic and comes out identical from both catalogues — verified: exactly one
distinct value per variant across every annotation block. It is the only safe cross-source
key of the three.

`dna_change` and `aa_change` are just the part after the colon, so they carry no prefix — but
they are still computed against that one transcript and mean nothing detached from it.

Two practical consequences: anything that matches or searches on an HGVS string has to
accept both namespaces, and the accession embedded in `hgvsc` is **versioned**
(`ENST00000831140.1`) while the `transcript_id` we store is not (`ENST00000831140`) — so the
two do not compare directly.

---

## 6. Partitioning the consequences table by source: considered and rejected

An earlier draft of this document proposed partitioning `snv__consequence` by `source`, so that
queries targeting one catalogue would read only half the table. **We measured it and rejected it.**
`source` ships as a plain nullable column added with `ALTER TABLE`. This section records why, so the
question does not have to be re-litigated.

All figures below are from the sandbox `radiant.snv__consequence` on StarRocks 4.0.11:
**73,040,174 rows, 5.5 GB, one partition, 10 buckets, `PRIMARY KEY(locus_id, symbol,
transcript_id)`, `colocate_with = radiant_radiant.query_group`.** 16.76M distinct loci, 4.36
consequences per locus.

| query | scan time |
|---|---|
| point lookup — `WHERE locus_id = ?`, 256 of 73M rows | **3.4 ms** (19 ms total) |
| the same lookup plus a low-cardinality predicate | **3.76 ms** — within noise |
| filtered `COUNT(*)` across all 73M rows | 113 ms |
| full scan + predicate + `GROUP BY`, 13.1M rows out | 132 ms |

**1. Nothing filters by source.** The only reader of `snv__consequence` in the pipeline is
`snv_consequence_filter_insert.sql`, an `INSERT OVERWRITE` full rebuild with no `WHERE` clause at
all — it reads the whole table, unnests `consequences` and re-aggregates. Across the entire pipeline
the WHERE/JOIN columns on this table are `locus_id`, `locus_hash`, `task_id`, `part`, `symbol`,
`transcript_id` and `consequence`; never `source`, `is_picked`, `is_canonical` or the MANE flags.
And §7 settles the display side: this is the table the variant page reads, and it must serve **both**
catalogues in full, so the page issues no source predicate either. The anti-join in §7's filter-table
proposal reads both sources too. There is no query to prune.

**2. It would make the dominant query slower, not faster.** The variant-page read is a point lookup.
`locus_id` bucket-prunes it to a single tablet and the primary-key short-key index narrows within
that tablet — 3.4 ms. The query profile shows per-tablet setup (`CreateSegmentIter` 560 µs,
`GetDelVec` 512 µs) is about a third of it. Two partitions over the same hash bucket means a lookup
with no source predicate — i.e. the variant page — opens **two tablets instead of one**. We would pay
roughly double on the hot path to speed up a query nobody issues. The predicate itself is free
regardless: a two-value dictionary-encoded column pushed into the scan, measurable only as noise.

**3. A 50/50 split is the weakest possible partition key.** Compare the precedent already in this
schema: `snv__consequence_filter` partitions on `is_deleterious`, which splits **99.6% / 0.4%**
(72.1 MB against 2.7 MB) — pruning there removes 96% of the data. That is what a partition key worth
having looks like. Source splits 50.5/49.5 (§2: 479,333 Ensembl blocks against 470,518 RefSeq). The
best case is halving a scan that already completes in 132 ms, inside a batch job measured in minutes.

**4. The primary-key cost is real, and the key gains nothing.** `snv__consequence` is a PRIMARY KEY
table, and StarRocks requires the partition column to be part of the primary key — so it would go
from three columns to four, on a table with `enable_persistent_index = true`. That is permanent index
growth on every upsert, and that upsert is the *only* deduplication on the write path (Iceberg
receives every annotation block verbatim, ~1.36M consequence rows per tumour-only WES sample). In
exchange, `source` adds **no discriminating power at all**: the two catalogues use disjoint
transcript namespaces — verified, 100% of the 67.3M non-empty `transcript_id` values are version-free
`ENST`, while RefSeq is `N[MRP]_`/`X[MRP]_` — so `(locus_id, symbol, transcript_id)` already
separates an Ensembl row from its RefSeq twin. The earlier claim that "the same variant/gene/
transcript can now legitimately exist once per source" is unreachable: if the transcript differs, the
existing three-column key already distinguishes them.

**5. It contradicts a decision we have already shipped.** SJRA-1823 deliberately stores
`source = NULL` for rows that belong to no catalogue — "we record unknown rather than guessing" (§3,
rule 4). Both partition columns in this codebase are declared `NOT NULL` (`is_deleterious`, `part`),
consistent with StarRocks' list-partitioning requirement, so partitioning would force a sentinel
value and undo that. This is not a corner case: **5,710,524 rows (7.8%)** of the current table have an
empty `symbol` *and* an empty `transcript_id` — exactly the class that resolves to a null source.

**Two claims from the original proposal, corrected.**

- *"The colocation group must be validated on the sandbox."* No validation needed, and no risk:
  `snv__consequence_filter` is **already** `PARTITION BY (is_deleterious)` **and** a member of the
  same `radiant_radiant.query_group`. Partitioning and colocation demonstrably coexist here today.
- *"Reloading or correcting one catalogue without touching the other."* Already possible.
  `snv_consequence_insert.sql` is an incremental upsert (`WHERE c.task_id IN (…)`) into a PRIMARY KEY
  table, and PK tables support `DELETE … WHERE source = 'RefSeq'` and key-wise replacement.
  Partitioning would only upgrade that to `TRUNCATE PARTITION` — faster, for an operation nobody runs
  on a schedule.

**What we do instead.** One statement, no rebuild, no reload, no downtime; primary key, distribution
and colocation all untouched, and nulls permitted so SJRA-1823's rule stands:

```sql
ALTER TABLE snv__consequence ADD COLUMN source VARCHAR(20) AFTER transcript_id;
```

All three columns this story adds to the table — `source`, plus SJRA-1827's
`mane_pair_transcript_id` and `scores_from_mane_pair` — ship as one runbook script,
`radiant/dags/sql/radiant/migrations/SJRA-1820_snv_consequences_add_columns.sql`. One story, one
table, and no consequence row is loaded between the statements, so splitting them per sub-task buys
nothing.

The `AFTER` position is mandatory rather than cosmetic: `snv_consequence_insert.sql` is a positional
`INSERT` with no column list, so the table has to match `init/snv_consequence_create_table.sql`
column for column. The integration suite does catch a mismatch — `_explain_insert` in
`tests/integration/dags/sql/test_queries.py` explains the statement as a real `INSERT INTO`, so a
column present on one side only fails with a count mismatch.

**If a single-source access pattern ever appears** — and is *measured*, not assumed — the cheap lever
is the sort key (`ALTER TABLE … ORDER BY`), which gives zone-map pruning without touching the key
semantics or requiring a reload. Partitioning stays the last resort, not the first.

---

## 7. What RefSeq rows do and do not carry

This is the section to read carefully, because the answer is not "RefSeq data is
incomplete" — it is "RefSeq data is complete for one class of filter and empty for another".

**Present on every RefSeq row, exactly as on Ensembl rows:** the consequence terms
(missense, frameshift, splice acceptor, …), the VEP impact (HIGH / MODERATE / LOW /
MODIFIER), the gene symbol, the biotype, the exon/intron position, and HGVS notation. These
are produced by VEP itself from the transcript model, so they are not tied to any external
annotation source. **These are also the portal's primary search filters.**

**Absent on RefSeq rows:** the external prediction scores — dbNSFP (SIFT, PolyPhen, CADD,
REVEL, phyloP, …) and gnomAD constraint (pLI, LOEUF) — because both are keyed on Ensembl
transcript identifiers. SpliceAI is keyed on gene symbol and does still apply to both.

### RefSeq is not redundant

Measured on the reference file, comparing the most severe impact each source assigns to the
same variant:

| | variants | share |
|---|---|---|
| Both sources agree on severity | 46,434 | 95.9% |
| Ensembl more severe than RefSeq | 1,662 | 3.4% (139 of them HIGH in Ensembl only) |
| **RefSeq more severe than Ensembl** | **348** | **0.7% (57 of them HIGH in RefSeq only)** |

And on gene attribution: **12.5% of variants are annotated against at least one gene symbol
that appears only in the RefSeq set** (11.7% the other way round).

So dropping RefSeq from search would hide 57 variants that RefSeq calls HIGH-impact and
Ensembl does not, and would make a meaningful set of gene searches return nothing. That is
a real clinical-review gap, not a rounding error.

### Proposal: everything for display, only what is new for search

The two tables serve different purposes and should be loaded differently.

**`snv__consequence` (display) — load both sources in full.** This is what the variant page
reads to show the transcript list; nothing is left out.

**`snv__consequence_filter` (search) — load Ensembl as today, plus the RefSeq annotations
Ensembl does not already provide.** In practice RefSeq repeats what Ensembl already says for
the vast majority of variants — for MANE transcripts the two catalogues produce an identical
gene, consequence and impact 99.9% of the time. Loading RefSeq wholesale would roughly
double the table without letting a user find a single additional variant. Loading only the
part that is new grows it by about 11%, and keeps everything RefSeq adds on its own —
including 79 HIGH-impact findings in genes such as `TPM2`, `SMARCD3` and `VPS11` that
Ensembl annotations alone would not surface.

*(Figures are from one trio; absolute counts scale with cohort size.)*

Two practical consequences:

- **The table's existing aggregation works as is.** RefSeq rows go through exactly the same
  grouping as Ensembl rows — only which rows are fed into it changes. No rework of the
  aggregation logic, and no change to the table's shape.
- **Score filters are unaffected.** Every RefSeq row that reaches the filter table is one
  Ensembl did not cover, so it has no scored counterpart to compete with. Those rows behave
  exactly like any variant dbNSFP does not cover today.

*Implementation note (SJRA-1828, which built it).*

The restriction is a single `LEFT ANTI JOIN` between the existing `UNNEST` and the existing
`GROUP BY` in `snv_consequence_filter_insert.sql`, against the Ensembl `(locus_id, symbol,
consequence)` keys of the same load. Nothing else about the statement changes — the aggregation, the
projection and the table's shape are untouched, as this section promised.

**`gr.source = 'RefSeq'` is a conjunct of the `ON` clause, not a `WHERE`.** An anti join keeps a left
row when no right row satisfies the *whole* condition, so that conjunct is what confines the
restriction to RefSeq: an Ensembl row can never satisfy it and passes through, and so does an
intergenic row, whose source is NULL under `resolve_source()` rule 4. Moving it to a `WHERE` would
change the meaning entirely.

**These duplicates do not collapse on their own,** which is the whole reason the join is needed. The
`GROUP BY` keys on the score columns, and a non-MANE RefSeq transcript carries nulls where the Ensembl
row it duplicates has dbNSFP values, so the two land in different groups and the duplicate survives as
a second row. This is pinned in
`tests/integration/dags/sql/test_snv_consequence_insert.py`: neutralising the join makes exactly that
unscored second `(TP53, missense_variant)` row reappear.

**Why `vep_impact` is not part of the key.** The obvious objection is that impact is derived from the
consequence, so it is redundant. It is not: VEP reports the impact of the **most severe term in the
block**, and the `UNNEST` copies that single label onto every term in it. Measured on the sandbox,
`intron_variant` occurs as MODIFIER, LOW *and* HIGH (52.0M rows), and the whole spread comes from
multi-term blocks — single-term blocks give 23 distinct terms and exactly 23 distinct term/impact pairs,
multi-term blocks give 25 terms and 49 pairs. `intron_variant` reaches HIGH only when it shares a block
with `splice_donor_variant` or `splice_acceptor_variant`.

Leaving it out is still correct, for a different reason: **no impact reachable through RefSeq can be
lost.** Take A, the most severe term of a RefSeq block — the term that sets that block's impact.

- If Ensembl does not report A for this `(locus, symbol)`, the RefSeq row for A survives the anti join
  and carries the block's impact in full.
- If Ensembl does report A, that Ensembl block contains A, so its impact is at least A's own rating —
  which *is* the RefSeq block's impact — and the Ensembl row already carries it.

The second step needs block impact never to fall below the rating of a term the block contains. Measured
over 99.2M unnested rows: zero counterexamples.

What adding `vep_impact` to the key would preserve is only the smeared label — an
`(intron_variant, HIGH)` RefSeq row where Ensembl already has `(intron_variant, MODIFIER)`. That HIGH
stays findable on the variant through its `splice_donor_variant` row, and Ensembl alone already produces
both of those rows today. So it would be more rows and not one more findable variant, which is exactly
what this restriction exists to avoid. Consistent with the MANE measurement above: across the 53,357
pairs, consequence and impact differ in **zero** cases, so any divergence at all can only come from
non-MANE transcripts.

**`snv_consequence_filter_insert_part.sql` needed no change.** It reads `snv__consequence_filter` with
`c.*`, not `snv__consequence`, so it inherits the restriction.

**No `source` column was added to the filter table.** After the restriction every row is either
Ensembl or "RefSeq where Ensembl was silent", and nothing in the query path distinguishes them; the
variant page reads the source from `snv__consequence`. Revisit only if the portal asks for it.

**The two cross-table dbt assertions still hold, and for a reason worth stating.**
`..._validate_subset_of_snv_consequence` only ever gets easier — the restriction removes rows, and
every surviving row still traces back to a base `snv__consequence` row. `..._validate_completeness_vs_snv_consequence`
asserts at *locus* grain, and no locus can be emptied by the restriction: a RefSeq key is dropped only
when an Ensembl key of the same locus is kept, and a locus with RefSeq annotations alone has nothing to
duplicate, so all of them survive.

### MANE gives us a free bridge between the two catalogues

With `--mane` enabled, VEP flags MANE transcripts on **both** sources — 106,985 MANE Select
and 714 MANE Plus Clinical annotation blocks in the file. Two separate columns are involved,
and their near-identical names invite confusion: **`MANE`** carries the flag (`MANE_Select` /
`MANE_Plus_Clinical` / empty), while **`MANE_SELECT`** carries an accession. The two labels are
mutually exclusive on a given transcript, but a gene can carry both on two different
transcripts (54 genes in the file, e.g. *NEB*), so the flags are two columns, not one.

The `MANE_SELECT` value is a **cross-reference to the other catalogue**:

- on an Ensembl row it holds the paired RefSeq accession (`ENST00000641515` → `NM_001005484.2`)
- on a RefSeq row it holds the paired Ensembl transcript (`NM_001005484.2` → `ENST00000641515.2`)

MANE is by construction one agreed transcript per gene, shared by Ensembl and RefSeq. So
VEP hands us an authoritative transcript equivalence for free — no external cross-reference
table needed. Coverage on the reference file:

| | variants | share |
|---|---|---|
| Have a MANE Select RefSeq annotation | 46,156 | 95.2% |
| Have a MANE Select Ensembl annotation | 45,900 | 94.7% |
| **Have both sides of the pair** | **45,900** | **94.7%** |

One limit of the bridge: VEP fills `MANE_SELECT` only on MANE **Select** rows. All 714 MANE
Plus Clinical blocks have it empty — the paired accession is in the `&`-joinable `RefSeq`
column instead, which is not a 1:1 join key. So Plus Clinical rows carry no pair and cannot
borrow scores through it. Harmless on the Ensembl side (those rows have their own `ENST`); it
leaves 357 RefSeq blocks unscored.

### Prediction scores on RefSeq rows: two options

Because RefSeq MANE rows are not loaded into the filter table (they add nothing), borrowed
scores are needed **only for display** on the variant page. Both options below have
*identical* coverage — only MANE rows can be paired, which is 53,628 of the 470,518 RefSeq
annotation blocks (~11%), covering 94.7% of variants. The choice is about *where the join
runs*, not about what a user sees.

| | Option 1 — materialise in the ETL | Option 2 — join at display time |
|---|---|---|
| Where the rule lives | one SQL insert | every portal query that shows scores |
| Storage | ~53.6k rows per file gain score values | none |
| Version stripping | done once, at load | done in each query |
| Freshness vs dbNSFP | snapshot taken at load | always current |
| Exports, reports, future consumers | get it for free | each must re-implement |

**Recommendation: option 1.** The decisive argument is not in the table: `snv__consequence`
*already* materialises the Ensembl scores at load time. Option 2 would give one table two
different freshness semantics — Ensembl rows frozen at load, RefSeq rows live at query time
— so the same variant's MANE pair could disagree after a dbNSFP refresh. Option 1 keeps a
single rule for the whole table and leaves the staleness question exactly where it already
is. It is also one `LEFT JOIN` in one place, versus a discipline the portal must maintain
across the variant page, exports, and anything built later, with a silent failure mode.

Add a boolean beside the scores — `scores_from_mane_pair` or similar — so the UI can label
those values as coming from the MANE twin rather than computed on the RefSeq transcript.

*Implementation note (corrected during SJRA-1824, which measured it).* The cross-reference
always carries a version suffix (`ENST00000641515.2`). The transcript identifier we store is
version-free on Ensembl rows (`ENST00000641515`) but **versioned on RefSeq rows** — all 470,518
RefSeq `Feature` values in the file are (`NM_000546.6`). So stripping only the cross-reference
gives a one-directional key:

| Direction | stripped `MANE_SELECT` vs the twin's raw `transcript_id` |
|---|---|
| RefSeq → Ensembl | 13,745 / 13,759 = **99.9%** |
| Ensembl → RefSeq | 0 / 13,745 = **0%** |
| Ensembl → RefSeq, twin's `transcript_id` also stripped | 13,745 / 13,745 = **100%** |

SJRA-1824 therefore stores **both** sides stripped, as two columns beside the raw values:
`mane_pair_transcript_id` (version-free `MANE_SELECT`) and `transcript_id_unversioned`
(version-free `Feature`). Joining `mane_pair_transcript_id` to `transcript_id_unversioned`
works in either direction, and `mane_pair_transcript_id` also lines up directly with dbNSFP's
`ensembl_transcript_id` and gnomAD constraint's `transcript_id`. The raw `transcript_id` keeps
its version for display, since that is the form a clinician cites.

*Implementation note (SJRA-1827, which built option 1).* Three things were settled while
implementing it, all measured rather than assumed.

**The join key is a source-keyed `CASE`, not a `COALESCE`.** Both enrichment joins in
`snv_consequence_insert.sql` now key on
`CASE WHEN c.source = 'RefSeq' THEN NULLIF(c.mane_pair_transcript_id, '') ELSE NULLIF(c.transcript_id_unversioned, '') END`.
The branches are not interchangeable, so this is the only correct form:
`COALESCE(mane_pair_transcript_id, transcript_id_unversioned)` would hand every *Ensembl* row its
paired `NM_…` and destroy the scores those rows get today, while the reverse order would never reach
the pair, because `transcript_id_unversioned` is always populated on a RefSeq row. `NULLIF` matters
because `strip_transcript_version` yields `''` — not NULL — for a declared-but-empty CSQ column, i.e.
on every non-MANE-Select row.

`ON d.ensembl_transcript_id IN (transcript_id_unversioned, mane_pair_transcript_id)` reads better and
is equally correct, since the two identifier namespaces are disjoint. It plans as a **NESTLOOP JOIN**
and was rejected on that basis. The `CASE` is hoisted into a `Project` above the scan, both joins stay
`HASH JOIN`, the `dbnsfp` join keeps `colocate: true`, and the expression is evaluated once across both
`ON` clauses via common-subexpression elimination — so inlining it twice costs nothing. Asserted in
`tests/integration/dags/sql/test_snv_consequence_insert.py`, because CI pins StarRocks 3.4.2 while this
was measured on 4.0.11.

**`scores_from_mane_pair` states the provenance of the join *key*, not that a value was found.** It is
`c.source = 'RefSeq' AND NULLIF(c.mane_pair_transcript_id, '') IS NOT NULL`, wrapped in a `COALESCE`
because `source` is NULL on intergenic blocks and the column is `NOT NULL DEFAULT "false"`. So a true
flag with a null `sift_score` means dbNSFP does not cover that locus — exactly as on an Ensembl row.
SJRA-1832 must therefore null-check each value and render "not available", rather than reading the flag
as "this row is scored".

The alternative — set the flag only when a value actually came back — was considered and dropped. It
would double as a silent-failure detector, but it conflates two different things: `gnomad_constraint`
is transcript-grained with no locus predicate, so its leg matches for nearly every paired row, while
`dbnsfp` is locus × transcript and only hits scored loci. A flag driven mostly by the constraint leg
would be read as "prediction scores are borrowed" and be wrong most of the time. The silent-failure
guard lives in the integration test instead, which compares the borrowed values against the twin's and
pins the two source tables independently.

**How this interacts with SJRA-1828.** `snv_consequence_filter_insert.sql` groups on the score
columns, so before the borrow a RefSeq MANE row and its Ensembl twin landed in *different* groups
purely because the RefSeq row's scores were all null. The borrow makes them collapse wherever symbol,
biotype and consequence agree. That is a real effect, but it is not what keeps the filter table small:
it only covers MANE pairs, and the rows that would actually have doubled the table are the **non-MANE**
RefSeq transcripts, which have no twin and therefore no scores to borrow. Those are removed by
SJRA-1828's restriction, not by the grouping. The two changes are complementary, and the restriction is
the load-bearing one — see the SJRA-1828 note below.

**`mane_pair_transcript_id` is materialised into `snv__consequence` too.** It is not needed for the
borrow — the join reads the Iceberg table, where SJRA-1824 already put it — but storing it makes the
borrow auditable in place: a RefSeq row joins straight back to its twin on
`mane_pair_transcript_id = twin.transcript_id`, with no version stripping in the consumer. That is what
SJRA-1830's "borrowed scores match their twin" assertion and the portal's RefSeq → Ensembl link both
need, and the stored `mane_select` cannot serve it because it keeps its version. `transcript_id_unversioned`
is deliberately *not* propagated: it is identical to `transcript_id` on Ensembl rows, and the direction
that matters (RefSeq → Ensembl) works with the pair column alone.

**Non-MANE RefSeq transcripts get no scores under either option.** Loading RefSeq-keyed
dbNSFP would be the only way to change that; it is not proposed, since those are alternative
transcripts dbNSFP largely does not cover under any identifier.

---

## 8. Decisions we need

| # | Question | Recommended default |
|---|---|---|
| 1 | Do we store RefSeq consequences at all, or drop them at ingestion? | Store them |
| 2 | Which source drives the variant page headline (gene, HGVS, transcript)? | Whichever VEP picked — the block must stay coherent. Expose `pick_source` so the portal can label and link correctly |
| 3 | Do RefSeq consequences participate in search filters? | Only where they report a gene or consequence Ensembl does not; score filters will not match them |
| 4 | Are RefSeq transcripts shown on the variant page transcript table? | Yes, labelled by source; MANE rows show borrowed scores marked as such, non-MANE rows show "not available" rather than blank |
| 5 | Do we backfill already-ingested experiments? | No — they contain no RefSeq data to recover; re-annotation is a separate discussion |
| 6 | Where do RefSeq rows get their prediction scores? | Materialised in the ETL via the MANE pair (option 1), flagged so the UI can label them |

Items 2–4 are product decisions; 1, 5 and 6 are technical with product impact.

---

## 9. Backward compatibility

- Files without a source column keep working unchanged; they simply resolve to a single
  source via the fallback rule.
- Existing rows already loaded in StarRocks have no source value. A one-shot `UPDATE` after the
  `ALTER TABLE` (§6) labels them as Ensembl, which is factually what they are — every file ingested
  to date was annotated against the Ensembl cache, and this is verifiable rather than assumed: all
  67.3M non-empty `transcript_id` values in the current table are `ENST`. Rows with no transcript at
  all keep a null source, per §3 rule 4.
- No portal change is *required* for the pipeline change to ship; the portal changes are
  what turn RefSeq data into something a user can see.

---

## 10. How QA validates this

1. **Classification is exhaustive.** For a merged file, every consequence row has a source
   of either Ensembl or RefSeq — except transcript-less (intergenic) rows, which are empty
   and are counted and reported, not silently dropped.
2. **Nothing is lost.** Consequence-row count in StarRocks matches the annotation-block
   count in the VCF; the Ensembl half matches, block for block, what the same file produces
   when ingested by the current pipeline.
3. **No regression on old files.** A previously-ingested non-merged file reproduces exactly
   the same rows as before, with `source = Ensembl` added.
4. **The primary key still separates the two catalogues.**
   `unique_combination_of_columns(locus_id, symbol, transcript_id)` still holds on a merged
   file — an Ensembl row and its RefSeq twin never collide on the key, because the two
   transcript namespaces are disjoint. This is the assumption doing the work that a source
   partition key would otherwise have done (§6), so it is asserted rather than assumed. The
   variant-page point lookup should also still touch a single tablet.
5. **The headline block is coherent.** For the ~1.4% of variants where VEP picked a RefSeq
   transcript, `pick_source` says RefSeq and every headline field — `transcript_id`,
   `hgvsc`, `hgvsp`, `dna_change`, `aa_change`, `symbol` — comes from that same transcript.
   No variant mixes fields from two transcripts.
6. **MANE flags are populated on both sources and agree.** Both an Ensembl and a RefSeq
   annotation carry the MANE Select flag for the same gene, and each side's cross-reference
   points at the other side's transcript. On the reference file this holds for 94.7% of
   variants; a large drop signals the `--mane` flag was lost upstream.
7. **Only the new RefSeq annotations are loaded for search.** The filter table grows by
   roughly 11%, not 100%; no gene/consequence combination appears twice for the same
   variant; and the RefSeq-only HIGH-impact findings (79 on the reference file, in genes
   such as `TPM2` and `SMARCD3`) are still findable.
8. **Borrowed scores are correct and labelled.** A RefSeq MANE row shows the same
   prediction scores as its Ensembl twin, marked as borrowed; a non-MANE RefSeq row shows
   none. A RefSeq row must never show scores that differ from its twin's.
9. **Volume is as predicted.** Row counts, load duration and table size are recorded for a
   merged file and compared against the ~2× expectation, so we catch a 5× surprise early.

---

## 11. Task breakdown

Tracked in Jira as **SJRA-1820** (story) with one sub-task per row below.

Ordered so that each task is shippable on its own and the pipeline work sits entirely ahead
of the portal work.

**Prerequisite**

| | Task |
|---|---|
| 0 | [SJRA-1821] Confirm decisions #2, #4 and #6 (§8) with the product owner: how the picked source is surfaced, how RefSeq rows are displayed, where borrowed scores come from |

**Pipeline**

| # | Task | Size | Depends on |
|---|---|---|---|
| 1 | [SJRA-1822] Fix the CSQ field lookup in extraction — `SOURCE` and `MANE_SELECT` are read under the wrong names today, so both columns are always empty | S | — |
| 2 | [SJRA-1823] Classify each consequence's source (§3) and populate it into Iceberg | M | 1 |
| 3 | [SJRA-1824] Populate `is_mane_select` / `is_mane_plus` from the `MANE` column, and store both sides of the pair **unversioned** as `mane_pair_transcript_id` and `transcript_id_unversioned` | S | 1 |
| 4 | [SJRA-1825] Add a merged-VCF test fixture and unit tests covering both file types | M | 2, 3 |
| 5 | [SJRA-1826] `snv__consequence`: add a `source` column via `ALTER TABLE` and label existing rows as Ensembl. Not partitioned by it — see §6 | S | 2 |
| 6 | [SJRA-1827] `snv_consequence_insert`: borrow dbNSFP and constraint scores for RefSeq MANE rows through the cross-reference, plus the `scores_from_mane_pair` flag (§7) | M | 3, 5 |
| 7 | [SJRA-1828] `snv_consequence_filter_insert`: load only the RefSeq annotations Ensembl does not already provide (§7) | M | 5 |
| 8 | [SJRA-1833] Add `pick_source` to `snv__variant` and its tmp / staging / partitioned equivalents (§5) | S | 2 |

**QA**

| # | Task | Size | Depends on |
|---|---|---|---|
| 9 | [SJRA-1829] Integration test on a merged file end to end — the checks in §10, including volume measurements | M | 5–8 |
| 10 | [SJRA-1830] dbt assertions in `data_qa`: source is always an accepted value, MANE pairs are consistent, borrowed scores match their twin | S | 5–8 |

**Portal** (separate repository, not blocking the above)

| # | Task | Size |
|---|---|---|
| 11 | [SJRA-1831] Show RefSeq transcripts on the variant page, labelled by source | M |
| 12 | [SJRA-1832] Label borrowed scores as coming from the MANE twin; show "not available" rather than blank for non-MANE RefSeq rows | S |

Tasks 1–3 are small and independent of any product decision, so they can start immediately.
Tasks 5 and 6 are metadata-only `ALTER TABLE`s (§6) and can ride a routine release; no table
rebuild or reload is needed anywhere in this story.

Two notes on scope: task 1 is a bug fix that happens to be a prerequisite, so it is worth
landing on its own rather than hidden inside task 2; and task 3 is in scope only because the
MANE bridge depends on it — it was outside the scope of this analysis when it started.
