# SJRA-1751 — Somatic SNV Tumor-Only — Grooming Summary

Short version of [the full technical analysis](./SJRA-1751-somatic-snv-tumor-only-ingestion.md).
Continues [SJRA-341](./SJRA-341-somatic-snv-ingestion.md), which built the Tumor-Normal path
and deferred Tumor-Only.

---

## Goal

Ingest somatic **Tumor-Only** (TO) variants — a tumor sample with no matched normal — so
they are queryable alongside the existing Tumor-Normal (TN) data, with their own variant
frequencies.

**In scope**: ETL, WES and WGS. Also three fields the tumor-only caller provides that we do
not currently capture, added to both the Iceberg and StarRocks occurrence tables:

- **SQ — somatic quality**, per call, for tumor and for normal. The tumor-only caller emits no
  germline-style quality field, so without SQ a tumor-only call carries no quality signal at
  all. This is the one field QLIN keeps that we do not.
- **AQ — systematic-noise score**, the signal the caller uses to flag noisy sites.
- **Hotspot, handled as a boolean.** The file marks known somatic sites with a simple
  present/absent flag. We already have a column for it, but it is never filled because we look
  for a differently-named field. Treating it as a boolean matches QLIN.

**Out of scope**: API and UI, CNV, cross-referencing TO against TN.

---

## Good news: most of the groundwork is already there

The previous ticket built the frequency tables with tumor-only columns already in place —
they are simply filled with zeros today. The occurrence table also already accepts a row
with no normal sample.

Practical consequence: **no new table, no new pipeline, no new DAG.** The work is a set of
changes to pieces that already exist. One of the three frequency layers needs no change at
all.

---

## The one thing everyone should know about this data

We characterised a real DRAGEN tumor-only WES file (~475,000 variants, 1 sample).

**A tumor-only file is mostly the patient's germline genome.** With no matched normal there
is nothing to subtract:

- **95%** of high-confidence calls sit at a germline allele fraction
- **99%** are already known variants in public databases

Two consequences the team should agree on:

1. **TO frequencies measure germline frequency, not somatic recurrence.** A common variant
   will show a TO frequency around 50%. That is the correct output — but it is a *different
   quantity* from the TN frequency, where the matched normal has already removed the
   germline. **TO and TN frequencies must never be compared, summed or averaged.** This
   matters most for whoever consumes them later.
2. TO produces roughly 4-5× the data volume of a comparable germline sample.

---

## Decisions already taken

| Decision | Choice |
|---|---|
| Where do TO variants live? | **Reuse the existing somatic occurrence table.** Normal-sample fields are simply left empty. Simpler for both ETL and API. |
| How do we know an analysis is tumor-only? | **A somatic analysis is tumor-only when it has exactly one tumoral sample and no normal sample.** No change needed to the clinical model. |
| How are TO frequencies counted? | **Separate from TN**, split by WES/WGS as we already do, using the same quality bar as TN. |
| Do TO variants enter the shared variant catalogue? | **Yes, treated like any other source.** No special filtering. |
| Do we adopt QLIN's frequency method? | **No** — we keep ours, which is more granular. |

---

## What needs to be built

Roughly in order of size:

1. **Fill in the tumor-only frequency calculation.** The columns exist; the logic that
   populates them does not. This is the bulk of the work.
2. **Let the ingestion handle a missing normal sample.** Today it refuses outright. Small,
   well-contained change.
3. **Add tumor-only frequency columns to the shared variant catalogue** so the numbers reach
   the portal.
4. **Capture SQ and AQ, and fix the hotspot reading logic.** Three fields added to the
   occurrence tables (Iceberg and StarRocks), plus a change to how we read hotspot — see the
   scope list above. Small and independent of the frequency work.
5. **Tests, data-quality checks and a test file.**

---

## Table changes

**No new tables.** Every change is an addition to a table that already exists, and two of the
three frequency layers need no schema change at all.

| Table | Change |
|---|---|
| Iceberg somatic occurrence *(raw parsed VCF)* | **3 new fields**, mirroring the StarRocks table below: **SQ** (somatic quality) for tumor and for normal, and **AQ** (systematic-noise score). The **hotspot reading logic changes** — look for the field name the file actually uses, and treat it as a boolean. Nothing changes to hold a tumor-only record: the normal-sample fields are already optional. |
| StarRocks somatic occurrence | **Reused as-is for tumor-only** — the normal-sample fields are simply left empty, since they are already optional. **Gains the same 3 fields: SQ for tumor and for normal, plus AQ.** **No column is added to mark tumor-only vs tumor-normal** — that is derived from the analysis instead. Also picks up the "known somatic site" (hotspot) annotation, which is always empty today because the file names it differently from what our code looks for. |
| Somatic frequency — staging | **No schema change.** The tumor-only columns already exist and are filled with zeros; only the logic that populates them changes. |
| Somatic frequency — per tenant | **No change at all.** Already handles tumor-only end to end. |
| Shared variant catalogue | **6 new columns** for tumor-only frequencies — carrier count, cohort size and ratio, for each of WES and WGS. Mirrors the tumor-normal columns already there. |
| Shared variant catalogue — partitioned copy | The same 6 columns. |
| Shared variant and consequence tables | **No schema change.** Tumor-only variants flow in like any other source; these tables are keyed by variant, so they grow only by variants not already seen. |

The one notable absence: because tumor-only vs tumor-normal is derived rather than stored,
the occurrence table needs no data migration and no backfill.

---

## One pre-existing defect to be aware of

Because the same tumor sample can be analysed both ways, the current somatic frequency
calculation will start misattributing data once tumor-only arrives: it groups by *case*
rather than by *analysis*, so a case containing both analyses is treated entirely as
tumor-normal.

This is a latent bug in code already in production — harmless while only TN exists, wrong as
soon as TO lands. **Decided: fixed in this ticket.** The fix is small and sits in the same
query the tumor-only frequency work has to change anyway, so splitting it out would mean two
tickets touching the same code.

Worth knowing for two reasons: it means this ticket also changes behaviour for existing
tumor-normal data, and it needs a regression test covering a case that carries both analyses.

---

## Open questions for this grooming

| # | Question | Why it needs a decision |
|---|---|---|
| 1 | **Zygosity for tumor-only** | The tumor-only caller does not determine zygosity — it reports every variant the same way. So the zygosity we would record is wrong for roughly half of the high-confidence calls. QLIN has the same flaw and does not correct it, so leaving it is *literal parity*; recording "unknown" instead is better data but a deliberate divergence. **Needs a call.** |
| 2 | **Germline background** | Given a TO file is ~99% known germline variants, do we want any filtering or flagging in the ETL, or is separating signal from background purely an API/UI concern? **QLIN has the same problem and does nothing about it**, so this would be new capability rather than catching up. |
| 3 | **Tumor-only WGS volume** | We measured WES. WGS is in scope and could be far larger. Worth measuring before we commit to a WGS rollout date. |

---

## Effort and risk

**Effort**: moderate. No new tables, no new pipeline, no clinical-model change — almost
entirely modifications to existing components, and one frequency layer already works.

**Main risks**

- **Interpretation, not engineering.** The largest risk is TO frequencies being read as
  somatic recurrence when they mostly reflect germline frequency. Worth flagging explicitly
  to whoever builds on them.
- **WGS volume** is the one unquantified unknown (open question 3). WES is well understood.
- Identifying tumor-only by sample count is simple, but cannot by itself distinguish a
  genuine tumor-only analysis from a tumor-normal one whose normal sample went missing
  upstream. Validation is planned to make that fail loudly rather than silently skew the
  frequencies.
