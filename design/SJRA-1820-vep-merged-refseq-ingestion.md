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
3. Partition the StarRocks consequences table **by source**, so queries that only want one
   transcript set read only that half of the table.

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
| StarRocks `snv__consequence` | New `source` column, **table partitioned by source** | Table rebuild + reload required |
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

## 6. Partitioning the consequences table by source

**What we get.** Queries that target one catalogue read only that partition — roughly half
the table — instead of scanning everything and filtering. Reloading or correcting one
catalogue can be done without touching the other. The table stays a single object, so
nothing downstream has to know about two tables.

**What it costs / what to watch.**

- The consequences table has to be **recreated and reloaded**; partitioning cannot be added
  to an existing table in place. This is a migration, not a rolling change.
- The partition key has to become part of the table's key columns. Practical effect: the
  same variant/gene/transcript can now legitimately exist once per source, which is exactly
  what we want, but it is a change in what "unique" means for that table.
- The consequences table is in a colocation group with the other variant tables (so joins
  stay local to each node). Adding partitions must not break that grouping — to be
  validated on the sandbox before we commit to the design.
- Two partitions only. This is deliberate: partitioning is for *pruning by source*, not for
  data lifecycle. It composes with, and does not replace, the existing part-based
  incremental loading.

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

### MANE gives us a free bridge between the two catalogues

With `--mane` enabled, VEP flags MANE transcripts on **both** sources — 106,985 MANE Select
and 714 MANE Plus Clinical annotation blocks in the file — and, importantly, the
`MANE_SELECT` value is a **cross-reference to the other catalogue**:

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

*Implementation note:* the cross-reference carries a version suffix (`ENST00000641515.2`)
while the transcript identifier we store and join on does not (`ENST00000641515`). Strip the
version before joining, or the join silently matches nothing.

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
- Existing rows already loaded in StarRocks have no source value. During migration they are
  labelled as Ensembl, which is factually what they are — every file ingested to date was
  annotated against the Ensembl cache.
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
4. **Partition pruning works.** A query restricted to one source reads only that partition
   (verifiable in the query profile), and returns the same rows as the equivalent filter on
   an unpartitioned copy.
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
| 3 | [SJRA-1824] Populate `is_mane_select` / `is_mane_plus`, and store the MANE cross-reference **unversioned** as its own column | S | 1 |
| 4 | [SJRA-1825] Add a merged-VCF test fixture and unit tests covering both file types | M | 2, 3 |
| 5 | [SJRA-1826] `snv__consequence`: add `source`, partition by it, migrate existing environments, verify the colocation group survives (§6) | M | 2 |
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
Task 5 is the only one requiring a migration and a reload, and should be scheduled
deliberately rather than bundled into a routine release.

Two notes on scope: task 1 is a bug fix that happens to be a prerequisite, so it is worth
landing on its own rather than hidden inside task 2; and task 3 is in scope only because the
MANE bridge depends on it — it was outside the scope of this analysis when it started.
