# CNV cohort frequencies — design

**Status:** draft for grooming. The matching rule and the algorithm are settled and measured against two
independent datasets (§6). §10 lists what is still open — all of it design coordination, none of it
performance.
**Ticket:** not yet created. Existing CNV-frequency tickets (SJRA-763, SJRA-779, SJRA-780) all concern the
**gnomAD-SV** join, i.e. an *external* population frequency. This document is about *our own cohort*
frequency, which does not exist today.
**Related:** `SJRA-1770-somatic-cnv-tumor-only-ingestion.md` (explicitly puts "cohort/population CNV
frequencies of our own" out of scope, and owns the `GET_CNV_ID` widening this design depends on),
`SJRA-1751-somatic-snv-tumor-only-summary.md` (the SNV cohort-split machinery we deliberately do *not*
need here — see §4.3), `docs/RADIANT.md` (partitioning).
**Existing precedent in the codebase:** `radiant/dags/sql/radiant/germline_cnv_occurrence_insert_partition_delta.sql`
lines 36–39 already implement an 80% reciprocal-overlap predicate for the gnomAD-SV join. SJRA-779 (Done)
settled that rule and its tie-break. This design reuses the same rule against ourselves.

---

## 1. Goal

Give every CNV occurrence an **in-cohort frequency** — "how many of our patients carry this CNV" — so the
portal can filter out common and recurrent-artifact events the way it already does for SNVs with
`germline__snv__variant_frequency`.

**In scope:** one new StarRocks table per analysis type, the SQL that populates it, its placement in the
DAG, and the matching rule.

**Out of scope:** gene-level and cytoband-level CNV frequencies (worth building, see §7.4, but a different
deliverable); somatic CNV frequencies until `somatic__cnv__occurrence` exists (SJRA-1770); portal/API
changes; gnomAD-SV behaviour, which is unchanged.

**The core difficulty in one sentence:** SNV frequency is a `GROUP BY` because `locus_id` is an exact key,
and CNV has no exact key — so frequency has to be defined over a *similarity relation*, and that relation
is not an equivalence relation.

---

## 2. Summary — the decisions, and how we build it

### Major decisions

- **Matching rule: 80% reciprocal overlap** on same chromosome and same `type`. Identical to the rule
  already used for gnomAD-SV (SJRA-779), so the platform has one definition of "the same CNV". (§4)
- **Frequency is keyed on `cnv_id`** — the bit-packed `(chromosome, start, length, type)` already stored on
  every occurrence row. `GROUP BY cnv_id` *is* the distinct-interval collapse, so no new key, no new column,
  no clustering, no anchor table. Joined from the portal as `occurrence.cnv_id = frequency.cnv_id`, exactly
  the shape SNV uses with `locus_id`. (§5.1)
- **Egocentric semantics** — frequency is a property of each distinct interval, not of a cluster. Two
  overlapping intervals may legitimately carry slightly different frequencies. This is a consequence of
  non-transitivity, not a defect. (§3.3)
- **Full recompute every import, once per `import_radiant` run** — *not* inside `import_part`. The
  relation crosses partitions so per-part counts are not additive, and `import_part` runs fan out in
  parallel, which would repeat the join N times against the same target table. (§8.2)
- **The occurrence table is never rewritten.** Frequency lives in its own small table keyed by distinct
  interval, so full recompute does not conflict with the partition-swap idempotency model. This is what
  makes "recompute everything" affordable architecturally, not just computationally. (§8.1)
- **Cost is a non-issue** — 5.4 s over 5.76 M calls / 8 464 samples, measured. The pruning that makes it
  cheap is *provably lossless* and was verified against brute force. (§5.3, §6)
- **Exact-interval matching is rejected**, and not on aesthetic grounds: measured on real DRAGEN WES data it
  mislabels ~4 CNV calls per patient as rare when they are ≥5% in-cohort. (§7.1)
- **Nothing is pre-computed on the occurrence table.** The bucket keys are derived in the frequency job, on
  the collapsed set. Storing them would compute them ~8× more often, in the write path of every part, for a
  step that already runs in under a second. (§5.1)
- **Stratify by `experimental_strategy`** (WGS/WXS) as SNV does. **No tumor-only / tumor-normal split** —
  somatic CNV is tumor-only only, so there is a single somatic cohort and none of the SNV verdict machinery
  is needed. (§4.3)
- **Denominators come from the sequencing tasks, never from the occurrences.** (§4.4)

### How we implement it

1. **DDL** — `germline__cnv__variant_frequency`, per-tenant, keyed on `cnv_id`. Two lines in
   `radiant_tables.py` and one file in `sql/radiant/init/`. (§8.1)
2. **The insert** — one `INSERT OVERWRITE` with four CTEs: collapse → binned self-join → expand to patients
   → divide by task-derived denominators. (§5)
3. **Placement** — one task at the end of `import_radiant`, after all `import_part` runs complete. (§8.2)
4. **Calibrate the threshold once** by emitting several overlap cutoffs, then ship the chosen one. (§5.5)
5. **QA** — dbt assertions in `radiant/data_qa/`, including the gnomAD cross-check that comes free. (§9.5)

**Dependency:** the widened `GET_CNV_ID` UDF from SJRA-1770 must land first. Today the UDF returns NULL for
any ALT that is not `<DUP>`/`<DEL>` and keys on `alternate` rather than `type`; a frequency keyed on
`cnv_id` inherits both defects. (§10.1)

---

## 3. Why CNV frequency is not SNV frequency

### 3.1 SNVs have an exact key; CNVs do not

`germline__snv__variant_frequency` works because `locus_id` identifies a mutation exactly. Two patients
either carry the same one or they do not, so counting is a `GROUP BY` and per-partition counts add up.

A CNV is an interval. Two patients can carry the same biological deletion while the caller reports
different coordinates for each — DRAGEN's boundaries snap to bin or exon-target edges, and which bins get
included varies with coverage. So `cnv_id` equality is *far too strict* to be the definition of "the same
CNV": it is the definition of "the same reported interval".

### 3.2 The rule, and why "reciprocal"

Two CNVs match when the overlapping region covers at least 80% of **each** of them:

```
overlap = max(0, min(end_a, end_b) - max(start_a, start_b))
match  ⟺  overlap ≥ 0.8 × len_a  AND  overlap ≥ 0.8 × len_b
```

The reciprocal part is what does the work. A one-directional test would let a 5 kb deletion nested inside a
1 Mb deletion count as a match, since the small one is 100% covered. Requiring both directions forces the
two events to be comparable in **size** as well as in position.

Useful consequence, used throughout §5.3: 80% reciprocal overlap implies `len_a / len_b ∈ [0.8, 1.25]` and
`|start_a − start_b| ≤ 0.2 × len_first`, where `len_first` is the length of whichever interval starts
**first**. It is **not** `0.2 × min(len_a, len_b)` — §5.3 has the counterexample, and getting this wrong
costs real matches.

### 3.3 Non-transitivity, and what it forces

The relation is symmetric and reflexive but **not transitive**. Concretely, three deletions all starting at
chr2:500 000:

| | length | vs D | vs E | vs F |
|---|---|---|---|---|
| **D** | 100 kb | — | ✗ | ✓ |
| **E** | 180 kb | ✗ | — | ✗ |
| **F** | 120 kb | ✓ | ✗ | — |

`F` matches both `D` and `E`; `D` and `E` do not match each other. (D vs E: overlap 100 kb, but 80% of E is
144 kb.)

So there is no well-defined set of "CNV variants" to group by, and any scheme that forces overlapping
intervals into groups has to make an arbitrary call. Two ways to respond:

- **Egocentric (chosen).** For each distinct interval, count the patients carrying anything that matches
  *it*. Well-defined, order-independent, no arbitrary grouping. Cost: two overlapping intervals can carry
  slightly different frequencies.
- **Clustered (rejected).** Assign a cluster id and group normally. Cheaper to query, but single-linkage
  chaining can swallow a chromosome arm, and one new sample can merge two clusters and silently change every
  frequency in them.

The egocentric choice is visible in the output and worth knowing about before someone files it as a bug: in
the DGV validation run, the ten most common intervals were ten near-identical ~45 kb deletions at chr1:72.3 Mb
carrying 3 730–3 741 patients each. Same biological event, marginally different match sets, marginally
different numbers. That is correct behaviour.

### 3.4 The consequence for the pipeline

The SNV design is two-tier: a per-`part` staging table, then a global rollup that sums it
(`germline_snv_variant_frequency_insert.sql`). That works because `locus_id` means the same thing in every
partition.

Reciprocal overlap breaks additivity — a patient in part 5 can match an interval in part 12, and no
per-part aggregate can see it. So the staging tier has no purpose here and the job is a single global pass.
§7.2 records the two alternatives we considered for preserving incrementality and why neither is needed.

---

## 4. The matching rule in full

### 4.1 The predicate

Exactly the predicate already in `germline_cnv_occurrence_insert_partition_delta.sql:36-39`, applied to
ourselves instead of to gnomAD:

```sql
WHERE GREATEST(0, LEAST(a.`end`, b.`end`) - GREATEST(a.`start`, b.`start`)) >= 0.8 * (a.`end` - a.`start`)
  AND GREATEST(0, LEAST(a.`end`, b.`end`) - GREATEST(a.`start`, b.`start`)) >= 0.8 * (b.`end` - b.`start`)
```

### 4.2 Matching on raw `type` — v1, and what it costs

**v1 requires `type` equality.** `GAIN` matches `GAIN`, `LOSS` matches `LOSS`, `CNLOH` matches `CNLOH`,
`GAINLOH` matches `GAINLOH`. No mapping, no derived column, nothing to keep in sync.

The known cost, stated here so it is a decision rather than an oversight: **`GAIN` and `GAINLOH` are the same
dosage event** — copy number is up in both, and the LOH detail lives in `mcn`. Requiring exact `type`
equality splits a common duplication region across two classes when some samples are called `GAIN` and
others `GAINLOH`, and **both halves are then understated**.

This is live, not hypothetical: production germline CNV data already contains `CNLOH` and `GAINLOH` (SJRA-1770
grooming). §10.2 carries the refinement and the one query needed to size it.

`CNLOH` is copy-neutral and correctly matches only itself under this rule — a patient with CNLOH does not
"have the same event" as a patient with a deletion at the same locus, so `type` equality is already the right
answer there. The only questionable pooling is GAIN/GAINLOH.

### 4.3 Stratification — and what we deliberately do *not* need

- **WGS and WXS must be separate**, more urgently than for SNV. Exome CNV boundaries are dictated by the
  capture design, so a WXS interval and a WGS interval are not comparable objects and reciprocal overlap
  between them is close to meaningless.
- **Germline and somatic separate** — different size regimes entirely, and different tables anyway.
- **No tumor-only / tumor-normal split.** Somatic CNV is **tumor-only only** — SJRA-1770 puts tumor-normal
  CNV out of scope, and tumor-only is *declared* there by `task_type = tumor_only_variant_calling` rather
  than derived from the task's aliquot set. So somatic CNV has a single cohort, and none of the
  `somatic_task_kinds` / `is_tumor_only` / `is_tumor_normal` machinery from
  `somatic_snv_staging_variant_freq_insert.sql` is needed or wanted here. This is a genuine simplification
  over the SNV path, and it follows from a decision already taken — not one this design makes.

Worth stating once, because it will look like an omission otherwise: **tumor-only CNV retains the patient's
inherited CNVs**, so the somatic in-cohort frequency is a germline+somatic mixture. That is not a flaw to be
fixed — it is precisely what makes it useful as a panel-of-normals-style artifact filter. Documented here so
nobody "corrects" it later.

### 4.4 Denominators

`pn` counts the patients who **could** have produced the call — taken from the sequencing tasks, never from
the occurrences. A patient sequenced with WGS who has no CNV in a region still belongs in `pn_wgs`. This is
the rule the somatic SNV frequency already follows and the reason its denominators are computed from
`staging_sequencing_experiment` rather than from the occurrence table.

### 4.5 Quality gating

The SNV equivalent gates on `filter = 'PASS'` and `tumor_ad_alt > 2`. For CNV:

- `filter` is **multi-valued** and its vocabulary **differs between DRAGEN versions** — 3.10.8 emits
  `cnvQual`/`cnvBinSupportRatio`/`cnvCopyRatio`/`LoDFail`, 4.2.4 emits `cnvQual`/`cnvLength`/`binCount`/
  `segmentMean`. The gate must be a union-aware predicate, not `= 'PASS'`.
- A `bc` (bin count) floor is worth adding. Ungated low-support segments inflate frequencies exactly in the
  noisy regions.

Both are gates on the *input* to the frequency job, so they can be tuned without touching the algorithm.

---

## 5. The algorithm

Four CTEs of a single `INSERT OVERWRITE`. Presented separately here with a worked example; §5.6 has the
assembled statement.

Worked example — five germline WGS patients on chr1:

| patient | chromosome | start | end | type |
|---|---|---|---|---|
| P1 | chr1 | 1 000 000 | 2 000 000 | LOSS |
| P2 | chr1 | 1 000 000 | 2 000 000 | LOSS |
| P3 | chr1 | 1 010 000 | 1 990 000 | LOSS |
| P4 | chr1 | 1 000 000 | 1 400 000 | LOSS |
| P5 | chr1 | 1 000 000 | 2 000 000 | GAIN |

P1 and P2 are byte-identical so they share a `cnv_id`. Call the four distinct ids **A** (P1, P2), **B**
(P3), **C** (P4), **D** (P5). Expected answer: A and B are the same event (3 patients), C and D are alone.

### 5.1 Step 1 — collapse to distinct intervals

```sql
distinct_cnv AS (
    SELECT
        cnv_id,
        ANY_VALUE(chromosome) AS chromosome,
        ANY_VALUE(`start`)    AS `start`,
        ANY_VALUE(`end`)      AS `end`,
        ANY_VALUE(`type`)     AS `type`,
        FLOOR(LOG2(ANY_VALUE(`end`) - ANY_VALUE(`start`)))                     AS len_bucket,
        FLOOR(ANY_VALUE(`start`)
              / POW(2, FLOOR(LOG2(ANY_VALUE(`end`) - ANY_VALUE(`start`)))))    AS pos_bin
    FROM {{ mapping.starrocks_germline_cnv_occurrence }}
    WHERE <quality gate, §4.5>
    GROUP BY cnv_id
)
```

`ANY_VALUE` is safe because `cnv_id` is an injective bit-packing of exactly these fields — that is the
point of keying on it. **`GROUP BY cnv_id` is the distinct-interval collapse.** Five rows become four.

Measured collapse: **8.1:1** on DGV, **15.2:1** on DRAGEN WES (§6).

**Why `len_bucket` / `pos_bin` are computed here and not stored on the occurrence table.** They are pure
functions of `start` and `end`, and they are needed on *distinct intervals* (711 k rows in the DGV test), not
on occurrences (5.76 M). Pre-computing them would calculate them ~8× more often, in the write path of every
`import_part` load, to speed up a step that measured under one second. They would also add two derived
columns to a table that already needs a backfill for the `cnv_id` change (§10.1), and derived state can
drift. There is no physical-layout benefit either: the join runs against the collapsed set, and the collapse
is a full scan by definition.

For threshold-calibration sweeps (§5.5) it *is* convenient to materialise the collapsed set as a real table
so the join can be re-run at several cutoffs without re-collapsing. That is an ops convenience, not a
production requirement.

### 5.2 Step 2 — the self-join

```sql
probe AS (
    SELECT
        d.cnv_id, d.chromosome, d.`type`, d.`start`, d.`end`,
        d.len_bucket + b.db                                    AS t_len_bucket,
        FLOOR(d.`start` / POW(2, d.len_bucket + b.db)) + p.dp   AS t_pos_bin
    FROM distinct_cnv d
    CROSS JOIN (SELECT -1 AS db UNION ALL SELECT 0 AS db UNION ALL SELECT 1 AS db) b
    CROSS JOIN (SELECT -1 AS dp UNION ALL SELECT 0 AS dp UNION ALL SELECT 1 AS dp) p
),
matches AS (
    SELECT p.cnv_id AS query_id, t.cnv_id AS match_id
    FROM probe p
    JOIN distinct_cnv t
      ON  t.chromosome = p.chromosome
      AND t.`type`     = p.`type`
      AND t.len_bucket = p.t_len_bucket
      AND t.pos_bin    = p.t_pos_bin
    WHERE GREATEST(0, LEAST(p.`end`, t.`end`) - GREATEST(p.`start`, t.`start`)) >= 0.8 * (p.`end` - p.`start`)
      AND GREATEST(0, LEAST(p.`end`, t.`end`) - GREATEST(p.`start`, t.`start`)) >= 0.8 * (t.`end` - t.`start`)
)
```

Two details that matter:

- **`pos_bin` is recomputed with the *target* bucket's width**, because bin width varies by bucket. Using
  the probe's own `pos_bin` against a neighbouring bucket would silently miss matches.
- **The nine probe keys are all distinct**, so a given pair can be found at most once. No `DISTINCT` needed
  downstream — verified, §6.3.

On the example: A vs B overlaps 980 kb ≥ 80% of both → match. A vs C overlaps 400 kb, below 80% of A's
1 Mb → rejected. A vs D shares bucket and bin but differs in `type` → never compared.

**Why nine explicit keys instead of a range predicate.** The obvious-looking alternative is
`t.len_bucket BETWEEN p.len_bucket - 1 AND p.len_bucket + 1` and similar for the position. A database
cannot hash a range, so that plan degrades to a nested loop. Measured on the DGV set (711 312 distinct
intervals, `COUNT(*)` only, so no write cost):

| variant | pairs found | time | correct? |
|---|---|---|---|
| **nine explicit keys (this design)** | **15 199 004** | **486 ms** | ✓ |
| `BETWEEN` on `len_bucket` + `BETWEEN` on `pos_bin` | 14 338 390 | 71 s | ✗ misses 5.7% |
| `BETWEEN` on `len_bucket` + `start ± 0.20·len` | 15 089 070 | 124 s | ✗ misses 0.7% |
| `BETWEEN` on `len_bucket` + `start ± 0.25·len` | 15 199 004 | 117 s | ✓ |

A *correct* range join is **240× slower**. Two of the three range variants are also silently wrong, which
is the more important lesson:

- **Ranging on `pos_bin` does not work at all.** `pos_bin` is computed with each row's *own* bucket width,
  so comparing a bucket-15 bin number against bucket-14 bin numbers compares different scales. It loses
  essentially every cross-bucket match — 861 196 of the 15 199 004 pairs are cross-bucket, and this variant
  found only 582 of them. No error, no warning, just a plausible-looking count that is 5.7% short.
- **Ranging on `start` works only with the right radius** — `0.25·len`, not `0.20·len`. See §5.3.

### 5.3 Why the binning is exact

The join is an equi-join on four columns with the overlap test as a cheap residual, which is what keeps
StarRocks from degrading to a nested loop. Both keys are derived from the geometry of the predicate, so
neither can drop a true match:

**Length.** `overlap ≥ 0.8·len_a` and `overlap ≤ min(len_a, len_b)` give `len_a/len_b ∈ [0.8, 1.25]`. Two
lengths within 1.25× of each other fall in the same `FLOOR(LOG2(len))` bucket or in adjacent ones, so
probing `±1` suffices.

**Position.** Take `start_a ≤ start_b` and let `Δ = start_b − start_a`. Then
`overlap ≤ end_a − start_b = len_a − Δ`; combined with `overlap ≥ 0.8·len_a` this gives **`Δ ≤ 0.2·len_a`**.
Note which length that is: the bound is governed by the interval that starts **first**, not by the shorter
one.

> **It is tempting to write `Δ ≤ 0.2·min(len_a, len_b)`, and that is wrong.** Counterexample:
> `a = [0, 100]`, `b = [20, 100]`, so `len_a = 100`, `len_b = 80`, `Δ = 20`. Overlap is 80, which is
> ≥ 0.8 × 100 and ≥ 0.8 × 80 — a genuine match — but `0.2 × min(100, 80) = 16 < 20`. This is not
> academic: a range join built on the `min` bound silently lost 109 934 of 15 199 004 pairs (§5.2).

Searching outward from a query interval `p`, the partner may start either before or after `p` and may be up
to 1.25× longer, so the safe search radius is **`0.25 × len_p`**.

Now the bin argument. With `b = FLOOR(LOG2(len_p))` we have `len_p < 2^(b+1)`, so the displacement is
strictly less than `0.25 × 2^(b+1) = 0.5 × 2^b`. Against each probed bucket:

| target bucket | bin width | max displacement | as fraction of a bin |
|---|---|---|---|
| `b + 1` | `2^(b+1)` | `< 0.5 · 2^b` | 0.25 |
| `b` | `2^b` | `< 0.5 · 2^b` | 0.5 |
| `b − 1` | `2^(b−1)` | `< 0.5 · 2^b = 2^(b−1)` | **< 1.0** (binding case) |

In all three the displacement is strictly less than one bin width of the *target* bucket, so the two bins
differ by at most 1 and `±1` suffices. The `b − 1` row is the tight one — at 80% there is no margin left
beyond it.

This is a proof, and it was checked twice empirically: against a full brute-force join (§6.3) and against an
independent range-join formulation (§5.2), both reproducing 15 199 004 pairs exactly.

**The scheme is not tied to 0.8, but it has a floor — and the floor is higher than it looks.** For a general
threshold `f` the displacement can reach `(1 − f)·2^(b+1)`, and the binding case above requires that to fit
inside `2^(b−1)`:

```
(1 − f) · 2^(b+1)  ≤  2^(b−1)   ⟺   (1 − f) ≤ ¼   ⟺   f ≥ 0.75
```

So **`pos_bin ±1` is valid only for `f ≥ 0.75`**. (`len_bucket ±1` is laxer — it needs the length ratio to
stay under 2 so the `LOG2` floors differ by at most 1, i.e. `f ≥ 0.5`; see Appendix B.4 — so the position
bin is what binds.) At `f = 0.7` the displacement reaches 1.2 bin widths
and at `f = 0.5` it reaches 2, and in both cases matches are dropped **silently**. Sweeping below 0.75
requires `pos_bin ±2`, which covers displacement up to two bin widths and is therefore good down to
`f = 0.5`. Worth a comment in the SQL next to the constant, because the failure mode is a plausible-looking
undercount with no error.

### 5.4 Step 3 — expand to patients

```sql
carriers AS (
    SELECT m.query_id AS cnv_id, s.patient_id, s.experimental_strategy
    FROM matches m
    JOIN {{ mapping.starrocks_germline_cnv_occurrence }} o ON o.cnv_id = m.match_id
    JOIN {{ mapping.starrocks_staging_sequencing_experiment }} s
      ON s.seq_id = o.seq_id
     AND s.tenant_code = %(tenant_code)s
     AND s.analysis_type = 'germline'
),
counts AS (
    SELECT
        cnv_id,
        COUNT(DISTINCT CASE WHEN experimental_strategy = 'wgs' THEN patient_id END) AS pc_wgs,
        COUNT(DISTINCT CASE WHEN experimental_strategy = 'wxs' THEN patient_id END) AS pc_wxs
    FROM carriers
    GROUP BY cnv_id
)
```

`COUNT(DISTINCT patient_id)` is load-bearing, not decoration. For `cnv_id` A, match A returns P1 and P2 and
match B returns P3 — three rows, three patients. Give P1 a second fragmented segment that also matches and
this CTE emits four rows for A; a plain `COUNT(*)` would report 4 patients out of 5. This is the single
easiest place in the design to introduce a silent overcount.

**If this step becomes the bottleneck**, replace it with bitmaps rather than restructuring anything:
pre-aggregate `BITMAP_UNION(TO_BITMAP(patient_id))` per `cnv_id`, then `BITMAP_UNION_COUNT` over the matched
set. Union-then-count *is* distinct-count, exactly (bitmaps on integers, not HLL). Measured: identical
results, equally fast today, but a 15 M-row intermediate instead of 156 M (§6.4). It is the first knob to
turn as the cohort grows, and it needs `patient_id` to be an integer.

### 5.5 Step 4 — denominators, and the threshold

```sql
denom AS (
    SELECT
        COUNT(DISTINCT CASE WHEN experimental_strategy = 'wgs' THEN patient_id END) AS pn_wgs,
        COUNT(DISTINCT CASE WHEN experimental_strategy = 'wxs' THEN patient_id END) AS pn_wxs
    FROM {{ mapping.starrocks_staging_sequencing_experiment }}
    WHERE analysis_type = 'germline' AND tenant_code = %(tenant_code)s
)
```

On the example, with all five patients WGS germline: `pn_wgs = 5`, so A and B report 3/5 = 0.60, C and D
report 1/5 = 0.20 — the expected answer.

**On the 0.8 cutoff.** It is conventional, and because reciprocal overlap turns out to do substantial work
(§7.1) the exact value matters. But do **not** ship a table with `pc/pn/pf` at four thresholds crossed with
every stratum — that is dozens of columns for no operational benefit. Instead:

1. **Calibrate once.** Keep the raw overlap fraction in `matches` and emit `pc` at several cutoffs via
   conditional aggregation in one pass. Compare against gnomAD-SV AF and against the threshold-crossing
   counts from §7.1. **Mind the `f ≥ 0.75` binning floor from §5.3:** a sweep of 0.8 / 0.85 / 0.9 is safe
   as written, but including 0.5 or 0.7 requires widening the probe to `pos_bin ±2` first — otherwise the
   low cutoffs return silently undercounted results and the calibration reaches the wrong conclusion.
2. **Ship one.** The production table carries the chosen threshold with full stratification.

### 5.6 The assembled statement

```sql
INSERT OVERWRITE {{ mapping.starrocks_germline_cnv_variant_frequency }}
WITH distinct_cnv AS ( ... §5.1 ... ),
     probe        AS ( ... §5.2 ... ),
     matches      AS ( ... §5.2 ... ),
     carriers     AS ( ... §5.4 ... ),
     counts       AS ( ... §5.4 ... ),
     denom        AS ( ... §5.5 ... )
SELECT
    cnv_id,
    pc_wgs, (SELECT pn_wgs FROM denom) AS pn_wgs,
    pc_wgs / NULLIF((SELECT pn_wgs FROM denom), 0) AS pf_wgs,
    pc_wxs, (SELECT pn_wxs FROM denom) AS pn_wxs,
    pc_wxs / NULLIF((SELECT pn_wxs FROM denom), 0) AS pf_wxs
FROM counts
```

`NULLIF(..., 0)` matters — a tenant with no WXS patients at all must yield NULL, not a division error.

---

## 6. Performance — measured, not estimated

Two independent datasets, deliberately chosen to be different in character.

| | **DGV** (public, multi-platform) | **DRAGEN WES** (real caller output) |
|---|---|---|
| samples | 8 464 | 7 151 |
| calls (deduplicated) | 5 760 822 | 3 608 115 |
| distinct intervals | 711 312 | 236 916 |
| **collapse ratio** | **8.1 : 1** | **15.2 : 1** |
| bins | 285 787 | 128 132 |
| avg k per bin | 2.49 | 1.85 |
| **max k per bin** | **1 026** | **125** |
| **Σk² (join-cost proxy)** | **16 860 738** | **1 322 678** |
| pairs produced | 15 199 004 | — |
| **end-to-end** | **5.4 s** | (12.7× cheaper by Σk²) |

DGV is the pessimistic case: it merges aCGH and sequencing studies at many resolutions, which manufactures
breakpoint diversity. Real single-caller output snaps to bin or target edges and collapses roughly twice as
well.

### 6.1 Skew

`max k = 1 026` on DGV, in chr6 LOSS around 32.5 Mb — **the MHC**, the most polymorphic region in the
genome. The worst bin in the genome lands exactly where biology says it should, and 1 026² is negligible.
On DRAGEN WES `max k = 125`, i.e. ~15 600 comparisons in the worst bin. There is no skew problem to
mitigate.

### 6.2 Scaling

| cohort | samples | calls | distinct | pairs | time |
|---|---|---|---|---|---|
| 12% | 1 058 | 692 071 | 143 705 | 541 991 | 1.0 s |
| 25% | 2 116 | 1 454 806 | 250 664 | 1 463 386 | 1.4 s |
| 50% | 4 232 | 2 922 419 | 439 778 | 4 949 180 | 2.1 s |
| 100% | 8 464 | 5 760 822 | 711 312 | 15 199 004 | 5.4 s |

Distinct intervals grow **sub-linearly** (8× samples → 4.95× distinct) because common CNVs are re-observed
rather than adding new keys. Pairs grow at roughly **N^1.6**. Extrapolating, ~50 000 samples is ≈260 M pairs
— minutes, for a job that runs once per import.

**Read the collapse ratio with the sample count.** It is not an intrinsic property of a dataset: it climbed
4.8 → 5.8 → 6.6 → 8.1 across the rows above as the cohort grew, because the numerator keeps growing while
the denominator saturates. An 8:1 at 8 464 samples and an 8:1 at 300 samples mean different things.

### 6.3 The binning was verified against brute force — whole genome, exact set equality

Full O(n²) self-join with the overlap predicate and **no bins at all**, run per chromosome and compared
pair-by-pair against the binned result. Not a count check: a `FULL OUTER JOIN` in both directions, so a
false negative cancelled by a false positive could not hide.

| | |
|---|---|
| brute-force pairs | **15 199 004** |
| binned pairs | **15 199 004** |
| **missed by binning (false negatives)** | **0** |
| **extra from binning (false positives)** | **0** |
| chromosomes with a discrepancy | **0 of 24** |
| brute-force runtime | 573 s (per chromosome), or **152 s** as one whole-genome query |

Every chromosome came out `0 / 0` individually. The heaviest was **chr6 at 2 484 259 pairs** — 2.6× the next
highest, and exactly where it should be, since chr6 LOSS around 32.5 Mb (the MHC) is also where `max k`
lives (§6.1).

Two further independent confirmations:

- **A different join strategy.** The range-join formulation in §5.2 (`start ± 0.25·len`, no position bins at
  all) reproduces 15 199 004 across the whole genome.
- **A different engine and storage architecture.** The entire pipeline was rebuilt from the source TSV on
  StarRocks **4.1.4 with local storage** (the original run was 4.0.13 shared-data on S3) and reproduced every
  intermediate exactly: 5 760 822 occurrences → 711 312 distinct intervals → 8 464 samples → 15 199 004
  pairs.

**Practical note on running the brute force yourself.** As a single whole-genome query it is 19.5 billion
comparisons and will OOM a small node — it killed a 7.65 GB one. Run it **per chromosome** (the join never
crosses chromosomes, so this costs no extra work) and **read-only**, as
`WITH brute AS (...) FULL OUTER JOIN binned`, materialising nothing. On a 14.4 GB BE that completes
comfortably in ~10 minutes.

### 6.4 Bitmaps are not needed yet

`BITMAP_UNION_COUNT` and plain `COUNT(DISTINCT)` produced **identical results (0 disagreements over 711 312
intervals)** and both ran in 2 s. The difference is the intermediate: **15 M rows vs 156 382 117**. Keep the
simple form; §5.4 records the switch.

### 6.5 Biological validation

Both datasets independently recovered known common CNVs, which is the strongest available evidence that the
matching logic is right:

- **DGV:** top hit chr1:72 300 000–72 346 000 LOSS, ~45 kb, in **44%** of samples — the well-characterised
  common deletion upstream of ***NEGR1***.
- **DRAGEN WES:** chr1:196 774 872 + 53 kb LOSS, 13 breakpoint variants, 1 719 carriers — the
  ***CFHR3/CFHR1*** common deletion. Plus a 36-variant ~40 kb GAIN cluster at chr11:~61.20 Mb.

Frequency distribution on DGV (≥1 kb), which is the shape germline CNV should have:

| class | share |
|---|---|
| singleton | 26.6% |
| < 0.1% | 21.0% |
| 0.1–1% | 25.2% |
| 1–5% | 12.3% |
| 5–25% | 12.2% |
| > 25% | 2.7% |

A large rare tail plus a real common fraction. **All-singletons would mean matching is broken;
all-common would mean over-merging.** Invariants clean: no `pf > 1`, no `pc > pn`, no `pc < 1`.

---

## 7. Alternatives considered

### 7.1 Exact-interval matching — rejected, with numbers

The obvious simplification: skip reciprocal overlap, `GROUP BY cnv_id`, count carriers. It is dramatically
simpler, needs no join, no binning, and is naturally incremental. It was measured properly on the DRAGEN WES
dataset (7 151 samples, events ≥1 kb, 204 613 intervals) against the RO result:

| exact says | RO says | intervals | share |
|---|---|---|---|
| < 1% | ≥ 1% | 20 063 | 9.8% |
| **< 1%** | **≥ 5%** | **3 384** | **1.7%** |
| < 1% | ≥ 25% | 4 | 0.002% |

Weighted by carriers — i.e. counting the calls a reviewer would actually see:

| | at ≥5% | at ≥1% |
|---|---|---|
| patient-level calls misranked | **29 635** | **194 734** |
| **per patient** | **≈ 4.1** | **≈ 27.2** |
| share of all calls | 0.8% | 5.4% |

**Every patient carries about four CNV calls that exact matching reports as rarer than 1% when they are
actually present in ≥5% of the cohort.** Those are precisely the calls that *survive* frequency filtering
when the frequency is wrong, so it is added triage burden on every case — and systematic, not random.

The mechanism is a common CNV with fuzzy breakpoints: the chr11 cluster above has 36 distinct breakpoint
variants, each individually near-singleton, collectively in ~24% of the cohort. Exact matching reports every
one of them at 1/7 151.

Prefer these threshold-crossing counts when justifying this work over the raw uplift ratios (median 1.6,
mean 15.2, p95 59, max 3 373). The ratios are computed over a skewed mixture and are maximised where the
denominator is 1; the headline 3 373× case was a sub-1 kb single-exon event.

### 7.2 Preserving per-partition incrementality — unnecessary

Two designs would keep per-`part` counts additive, and both were dropped once the cost was measured:

- **An append-only anchor catalog.** A permanent table of reference intervals; a new CNV either matches an
  existing anchor at ≥80% or becomes a new one, and anchors never move. Counts per anchor are then additive.
  Cost: anchor boundaries depend on arrival order (§3.3's D/E/F example resolves differently depending on
  which arrives first), and it is a stateful table to maintain and migrate.
- **A discrete surrogate key** — gene or fixed genomic bin — restoring exact-key semantics.

Both solve a performance problem that does not exist. **Do not re-propose either as a cost measure.** The
surrogate-key idea does have independent product value; see §7.4.

### 7.3 Clustering — rejected

Covered in §3.3. Single-linkage chaining plus instability under new data.

### 7.4 Gene-level and bin-level frequencies — complementary, not a substitute

"Percentage of the cohort with a LOSS affecting *TP53*" is a discrete key, fully incremental, immune to
breakpoint noise, and much closer to the question a clinician actually asks. The occurrence table already
carries `symbol[]` and `nb_genes`, so it is cheap.

It answers a **different question** and does not replace event-level frequency — a gene-level number cannot
tell you whether *this* patient's specific interval is common. Worth a separate ticket, and it is where the
fixed-bin variant also belongs (10 kb bins × type gives a genome-wide "how noisy is this region" track,
essentially free).

### 7.5 Breakpoint-distance matching — worth revisiting later

Reciprocal overlap is *too permissive* on large events: two 10 Mb deletions offset by 1 Mb still pass at 80%
despite differing by a megabase. A `|Δstart| ≤ X ∧ |Δend| ≤ X` conjunct alongside the RO test would fix it.
Not needed for the first version — events that large are rare (0.1% of DGV calls exceed 1 Mb) — but it is
the natural refinement if large-event frequencies look too smeared.

---

## 8. Tables, wiring, placement

### 8.1 The table

Per-tenant, mirroring `germline__snv__variant_frequency`:

```sql
CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_germline_cnv_variant_frequency }} (
   `cnv_id` BIGINT NOT NULL,
   `pc_wgs` BIGINT,
   `pn_wgs` BIGINT,
   `pf_wgs` DOUBLE,
   `pc_wxs` BIGINT,
   `pn_wxs` BIGINT,
   `pf_wxs` DOUBLE
)
DISTRIBUTED BY HASH(`cnv_id`) BUCKETS 10
```

Two entries in `radiant/tasks/data/radiant_tables.py`:

```python
STARROCKS_RADIANT_PER_TENANT_MAPPING = {
    ...
    "starrocks_germline_cnv_variant_frequency": "germline__cnv__variant_frequency",
}
```

and the table name added to `init_starrocks_tables.py`.

**On colocation:** `germline__snv__variant_frequency` sets `colocate_with` on the query group so the portal's
`locus_id` join is local. The CNV equivalent needs care — `germline__cnv__occurrence` is
`DUPLICATE KEY(part, seq_id, task_id, cnv_id) PARTITION BY (part)` with no explicit `DISTRIBUTED BY`, so it
is not currently bucketed on `cnv_id` and a colocate group cannot simply be declared. Treat colocation as a
follow-up optimisation, measured, not assumed.

**Affected / not-affected split:** `germline__snv__variant_frequency` carries `*_affected` and
`*_not_affected` variants. Whether CNV needs the same is a portal question — the SQL shape is identical if
so. Left out of the DDL above deliberately, pending that answer.

**Somatic:** `somatic__cnv__variant_frequency` with the same column set as above — a **single cohort**, no
`_tn_`/`_to_` prefixes, because somatic CNV is tumor-only only (§4.3). Blocked on `somatic__cnv__occurrence`
(SJRA-1770).

### 8.2 Where it runs

**One task at the end of `import_radiant`**, after all `import_part` `TriggerDagRunOperator` runs complete.

Note the temptation and why to resist it: the SNV *rollup* already runs inside `import_part`
(`import_part.py:379-381`), as a full-table `INSERT OVERWRITE`. That is fine there because it is a `SUM` over
a staging table. The CNV job is the expensive self-join, `import_part` runs fan out in parallel, and they
would each redo the whole join against the same target table. It is also cross-part by nature, so a per-part
DAG is the wrong home for it regardless of cost.

`INSERT OVERWRITE` gives atomic replacement, so the portal never observes a partially rebuilt table.

### 8.3 Multi-tenancy

The occurrence table has no `tenant_code` column — isolation comes from the per-tenant database. So the job
runs once per tenant against that tenant's database, and the only explicit `tenant_code` filters are on
`staging_sequencing_experiment` (numerators and denominators). Same arrangement as the somatic SNV frequency;
the comment block in `somatic_snv_staging_variant_freq_insert.sql:60-65` explains the reasoning.

---

## 9. Known limitations, and the QA that catches them

### 9.1 Fragmented segments will undercount

DRAGEN routinely reports one biological event as several adjacent segments. Patient A has one clean 1 Mb
deletion; patient B has the same region as 400 + 300 + 300 kb. Compared segment-by-segment, **none** of B's
pieces reaches 80% of A's event, so B is not counted — and the undercount is worst on the large recurrent
events we most want to catch.

**The fix, when we take it (see §10.3):** compare patient-to-event rather than segment-to-segment. For query
interval `E` and candidate patient `P`, let `I = Σ` bp of P's same-`type` segments intersecting `E`, and
`L = Σ` length of exactly those segments; require `I ≥ 0.8·len(E)` **and** `I ≥ 0.8·L`. Keep the query side
unmerged so `cnv_id` still joins.

### 9.2 `GAIN` / `GAINLOH` split

§4.2. A known understatement in both classes wherever a common duplication is called both ways. §10.2.

### 9.3 Same-capture-kit is load-bearing for WXS

Exome CNV frequency is only meaningful across samples whose targets are identical. Different kits mean
different callable regions and the denominator silently overstates. Fine for a single-kit cohort; a
documented caveat otherwise, and a real correctness issue if kits diversify.

### 9.4 Small cohorts

At clinical scale these are artifact filters, not population genetics. Consider suppressing `pf` below a
minimum `pn` rather than showing a 1/12 as "8%".

### 9.5 Recurrent artifacts are a feature

Centromeres, telomeres, segmental duplications, and noisy single-exon targets will show `pf` near 1. A 120 bp
"GAIN" at 47% of samples in the DRAGEN WES data is a recurrent coverage artifact, not a real duplication
polymorphism. Surfacing it loudly is exactly what this table is for.

### 9.6 QA assertions worth adding to `radiant/data_qa/`

- **Cross-check against gnomAD.** The occurrence table already carries `gnomad_af` for the same events, so
  correlating our in-cohort frequency against it costs nothing and is the best end-to-end validation of the
  matching logic. Systematic deviation means either an RO bug or a genuine cohort artifact.
- **Distribution shape** — if nearly everything is a singleton, matching is broken (§6.5).
- **Invariants** — `pf ≤ 1`, `pc ≤ pn`, `pc ≥ 1`, and no NULL `cnv_id`.
- **`cnv_id` coverage** — every occurrence row should join to exactly one frequency row. A miss means the
  UDF returned NULL (§10.1).

---

## 10. Open items and dependencies

### 10.1 `GET_CNV_ID` must be widened first — blocking

Today `CNVIdUDF` packs type in 1 bit, keys on `alternate`, and **returns NULL for any ALT that is not
`<DUP>`/`<DEL>`**. A frequency keyed on `cnv_id` inherits both problems: CNLOH/GAINLOH get a NULL key, and
the legacy VCF 4.2 multi-allelic `<DEL>,<DUP>` spelling of LOH keys as a plain loss.

SJRA-1770 already owns the fix — 3 bits of type (61–63), 5 of chromosome (56–60), 28 of start (28–55), 28 of
length (0–27), keyed on `type` instead of `alternate`. **Every germline `cnv_id` changes**, so the UDF
release, both existing SQL call sites, and the germline backfill land together. This design should follow
that, not race it.

### 10.2 Should `GAIN` and `GAINLOH` be pooled?

v1 says no (§4.2). Before deciding, size it — one query on the production germline table:

```sql
SELECT `type`, COUNT(*) AS n, COUNT(DISTINCT cnv_id) AS n_distinct
FROM {{ mapping.starrocks_germline_cnv_occurrence }}
GROUP BY `type` ORDER BY n DESC;
```

If `GAINLOH` is a rounding error next to `GAIN`, leave v1 as it is. If it is material, the fix is a derived
`match_class` expression in the collapse CTE — one `CASE`, defined once, referenced by the join:

```sql
CASE WHEN `type` IN ('GAIN','GAINLOH') THEN 'GAIN' ELSE `type` END AS match_class
```

Note this makes a `GAIN` and a `GAINLOH` at identical coordinates two different `cnv_id`s that match each
other, which is the intended behaviour. `CNLOH` should stay its own class either way — it is copy-neutral, so
it genuinely is not "the same event" as a deletion. Whether clinicians want the pooling is a product
question, not a technical one.

### 10.3 Per-patient union overlap — decide in or out for v1

§9.1 describes the fix for fragmented segments. It is a real accuracy gain and it costs nothing extra to
compute, but it complicates the collapse (the carrier side is per-patient, so it must be pre-merged per
sample before collapsing). Recommendation: **ship segment-level first**, then measure how much union
overlap adds using the same threshold-crossing method as §7.1. If it moves the per-patient misranking count
materially, take it.

### 10.4 Threshold calibration

§5.5, with the `f ≥ 0.75` binning floor from §5.3 — widen the probe to `pos_bin ±2` before sweeping any
cutoff below that, or the low end of the sweep is silently undercounted. One-off exercise, needs real cohort
data, should happen before the table is exposed in the portal.

### 10.5 Portal side

Not scoped here. The join is `occurrence.cnv_id = frequency.cnv_id`, structurally identical to the existing
`locus_id` join, so this should be small — but it is a separate ticket and the facet/filter definitions need
someone to own them.

### 10.6 Somatic

Blocked on `somatic__cnv__occurrence` (SJRA-1770). Once it exists the somatic frequency is the germline SQL
with the table names swapped and `analysis_type = 'somatic'` — **no cohort-split logic**, since somatic CNV
is tumor-only only (§4.3). The one thing to carry over from the somatic SNV path is the denominator rule
(§4.4), not the verdict CTEs.

---

## Appendix A — diagnostic queries

Run these against any new CNV dataset before assuming the cost profile above transfers. StarRocks dialect;
Spark equivalents differ only in `pow`/`log2` casing and needing `cast(k AS bigint)` in `sum(k*k)` to avoid
integer overflow.

**A.1 Collapse ratio.** Report it *with* the sample count (§6.2).

```sql
SELECT COUNT(*)                                    AS n_occ,
       COUNT(DISTINCT patient_id)                  AS n_samples,
       COUNT(DISTINCT cnv_id)                      AS n_distinct,
       ROUND(COUNT(*) / COUNT(DISTINCT cnv_id), 2) AS collapse_ratio
FROM <occurrence table>;
```

**A.2 Skew — the actual cost driver.** `sum_k_squared` estimates the join output directly; multiply by 2–3×
for the `±1` neighbour probes. Reference: 16.9 M at 711 k distinct intervals → 2 s.

```sql
SELECT COUNT(*)         AS n_bins,
       MAX(k)           AS max_k,
       ROUND(AVG(k), 2) AS avg_k,
       SUM(k * k)       AS sum_k_squared
FROM (
    SELECT COUNT(*) AS k
    FROM (SELECT DISTINCT chromosome, `type`,
                 FLOOR(LOG2(`end` - `start`)) AS len_bucket,
                 FLOOR(`start` / POW(2, FLOOR(LOG2(`end` - `start`)))) AS pos_bin
          FROM <occurrence table> WHERE `end` - `start` >= 1) d
    GROUP BY chromosome, `type`, len_bucket, pos_bin
) g;
```

**A.3 Brute-force equivalence check.** Run the binned join and an unbinned O(n²) join restricted to one
small chromosome (chr22 ≈ 15 k distinct intervals is a good size) and confirm both directions are empty:

```sql
SELECT COUNT(*) AS missed_by_binning
FROM brute b LEFT JOIN matches m ON m.query_id = b.query_id AND m.match_id = b.match_id
WHERE m.query_id IS NULL;
```

**A.4 What reciprocal overlap is buying.** The §7.1 numbers. `pc_exact` is the carrier count of the exact
interval; `pc_ro` the count after matching. The threshold-crossing counts are the quotable figures; weight
by `pc_exact` to convert intervals into patient-level calls.

```sql
SELECT COUNT(*) AS n_intervals,
       SUM(CASE WHEN pc_exact / :n_samples < 0.01
                 AND pc_ro    / :n_samples >= 0.05 THEN 1 ELSE 0 END)        AS rare_to_common_5pct,
       SUM(CASE WHEN pc_exact / :n_samples < 0.01
                 AND pc_ro    / :n_samples >= 0.05 THEN pc_exact ELSE 0 END) AS patient_calls_misranked
FROM uplift
WHERE q_len >= 1000;
```

**A.5 Sanity-check the extreme corrections.** Any interval whose frequency moves a lot should have matches
whose lengths sit inside the band the predicate forces. **`min_len_ratio ≥ 0.8` and `max_len_ratio ≤ 1.25`
are invariants** — a violation is a bug in the predicate, not a biological finding. Also useful: real
fuzzy-breakpoint common CNVs have many matched intervals (13–36 in the observed cases), whereas a spurious
pairing has one partner.

---

## Appendix B — the two bucket keys from first principles

§5.3 gives the proof compactly. This appendix walks the same ground slowly, for anyone meeting the scheme
for the first time or debugging it later. **B.1–B.5** cover `len_bucket`, **B.6–B.11** cover `pos_bin`,
which is the fiddlier of the two.

### B.1 What the formula computes

```sql
len_bucket = FLOOR(LOG2(length))
```

`LOG2(x)` answers *"2 raised to what power gives me x?"* — `LOG2(8) = 3` because `2³ = 8`, and
`LOG2(1024) = 10`. For anything that is not an exact power of two you get a decimal: `LOG2(1000) = 9.966`.
`FLOOR` then discards the decimal, always rounding down.

Together, `FLOOR(LOG2(x))` is **the exponent of the largest power of 2 that fits inside x** — equivalently,
the position of the highest 1-bit in the binary representation. `1000` is `1111101000` in binary: ten
digits, highest bit at position 9, so `FLOOR(LOG2(1000)) = 9`.

### B.2 The bands

Bucket `b` holds every length from `2^b` to `2^(b+1) − 1`:

| bucket | band | | bucket | band |
|---|---|---|---|---|
| 8 | 256 – 511 | | 14 | 16 384 – 32 767 |
| 9 | 512 – 1 023 | | 15 | 32 768 – 65 535 |
| 10 | 1 024 – 2 047 | | 16 | 65 536 – 131 071 |
| 11 | 2 048 – 4 095 | | 17 | 131 072 – 262 143 |
| 12 | 4 096 – 8 191 | | 18 | 262 144 – 524 287 |
| 13 | 8 192 – 16 383 | | 20 | 1 048 576 – 2 097 151 |

Real lengths dropped into it:

| length | `LOG2` | bucket |
|---|---|---|
| 300 bp | 8.229 | 8 |
| 500 bp | 8.966 | 8 |
| **511 bp** | **8.997** | **8** |
| **512 bp** | **9.000** | **9** |
| 1 000 bp | 9.966 | 9 |
| **1 023 bp** | **9.999** | **9** |
| **1 024 bp** | **10.000** | **10** |
| 1 200 bp | 10.229 | 10 |
| 45 643 bp | 15.478 | 15 |
| 53 285 bp | 15.702 | 15 |

Bands widen as lengths grow, which is the point: 100 bp is an enormous difference for a 500 bp event and
irrelevant for a 50 kb one. **Every band spans exactly a factor of 2**, so they are equal-sized in the only
sense that matters — proportionally.

### B.3 Why matching lengths are within 1.25× — the derivation

Let `O` be the length of the overlapping region. 80% reciprocal overlap is two requirements at once:

```
O ≥ 0.8 × len_A          and          O ≥ 0.8 × len_B
```

The bridge to length is a fact that is obvious once stated: **the shared region lies inside both intervals,
so it cannot be longer than either of them.**

```
O ≤ len_A                and          O ≤ len_B
```

The best possible case is perfect nesting — the smaller interval lying entirely inside the larger — where
`O` equals the whole smaller interval and cannot do better.

```
A (1000 bp)   ████████████████████
B  (800 bp)       ████████████████
                  └─ overlap = 800, all of B
```

Chain the first requirement with the second bound:

```
0.8 × len_A  ≤  O  ≤  len_B      ⟹      0.8 × len_A ≤ len_B      ⟹      len_A / len_B ≤ 1.25
```

In words: **B must be at least 80% the size of A**, because if it were smaller then even perfectly nested
it would not be long enough to cover 80% of A. Repeat starting from the other requirement and you get
`len_A / len_B ≥ 0.8`. Together:

```
0.8  ≤  len_A / len_B  ≤  1.25
```

`0.8` and `1.25` are reciprocals — it is one rule told from each interval's point of view, symmetric
around 1 because neither interval is special.

Checked on numbers, assuming the best possible placement each time:

| len_A | len_B | best `O` | `O ≥ 0.8·len_A`? | `O ≥ 0.8·len_B`? | ratio | can match? |
|---|---|---|---|---|---|---|
| 1 000 | 900 | 900 | 900 ≥ 800 ✓ | 900 ≥ 720 ✓ | 1.11 | yes |
| 1 000 | 800 | 800 | 800 ≥ 800 ✓ | 800 ≥ 640 ✓ | **1.25** | yes — with zero margin |
| 1 000 | 700 | 700 | 700 ≥ 800 ✗ | — | 1.43 | **no, at any position** |
| 1 000 | 500 | 500 | 500 ≥ 800 ✗ | — | 2.00 | **no, at any position** |

The 1 000 / 800 row passes only under perfect nesting; shift B by one base pair and the overlap falls to
799 and it fails. That is why 1.25 is an exact boundary, not an approximation.

### B.4 Why `±1` is necessary and sufficient

Take `LOG2` of the ratio bound. Logs turn division into subtraction:

```
LOG2(0.8) ≤ LOG2(len_A) − LOG2(len_B) ≤ LOG2(1.25)
  −0.322  ≤ LOG2(len_A) − LOG2(len_B) ≤  +0.322
```

So `| LOG2(len_A) − LOG2(len_B) | ≤ 0.322`. The remaining step is that **two numbers less than 1 apart have
floors that differ by at most 1**. That is worth seeing rather than asserting.

`FLOOR` chops the number line into unit-width cells, one per integer. The boundaries are spaced **exactly
1.0 apart** — that is the fact doing all the work:

```
        9                    10                   11                   12
        ├────────────────────┼────────────────────┼────────────────────┤
        └──── FLOOR = 9 ─────┘└──── FLOOR = 10 ───┘└──── FLOOR = 11 ────┘
```

Slide a window of width 0.322 anywhere along it:

```
        9                    10                   11
        ├────────────────────┼────────────────────┼
   (a)   ├──┤                                          both in cell 9   → 9, 9    diff 0
   (b)                    ├──┤                         crosses one line → 9, 10   diff 1
   (c)                       ├──┤                      both in cell 10  → 10, 10  diff 0
```

Wherever it lands, the window covers **at most one** boundary — it is 0.322 wide and the boundaries are
1.0 apart, so there is no room for two. Each boundary crossed adds exactly 1 to the floor difference, so
the difference is 0 or 1 and nothing else.

Floors 2 apart would need one point in cell 9 and the other in cell 11. Even the tightest such pair is more
than 1.0 apart, because getting from cell 9 to cell 11 means traversing the whole of cell 10:

```
        9                    10                   11
        ├────────────────────┼────────────────────┼
                          x                       y
                        9.999                   11.000
                          └───────── > 1.0 ──────┘
                                     ↑
                          must cross ALL of cell 10
```

Not unlikely — arithmetically impossible for a 0.322-wide window. Equivalently, as a counting rule:
`FLOOR(y) − FLOOR(x)` is exactly *the number of integers strictly between x and y*, and at most one integer
fits in a gap of 0.322.

Since `0.322 < 1`:

> Two matching intervals are in the same bucket, or in adjacent buckets. Never further.

That sentence is the whole justification for probing `±1`. Three cases:

- **Same bucket.** 45 643 and 53 285 — ratio 1.167, `LOG2` 15.478 and 15.702, difference 0.224, both floor
  to 15.
- **Adjacent buckets — this is why `±1` exists.** 1 000 and 1 200 — ratio 1.20, comfortably matchable.
  `LOG2` 9.966 and 10.229, a difference of only 0.263 — but the boundary at 10.000 falls between them, so
  they floor to **9** and **10**. Joining on equal buckets alone would lose this pair. With `±1`, the 1 000
  probes buckets 8/9/10 and finds it.

  ```
          9                    10                   11
          ├────────────────────┼────────────────────┼
                            ├──┤
                         9.966  10.229
                       (1 000 bp) (1 200 bp)
                        FLOOR 9   FLOOR 10
  ```
- **Two buckets apart — correctly pruned.** 1 000 and 3 000 — ratio 3.0, cannot pass 80% RO at any
  position. `LOG2` 9.966 and 11.551, difference 1.585, buckets 9 and 11. Rejected before the overlap test
  runs.

### B.5 Two things not to misread

**The length condition is necessary, not sufficient.** A ratio inside `[0.8, 1.25]` means the pair is *not
ruled out by size*. It does **not** mean the intervals match — two 1 000 bp deletions at opposite ends of
chr1 have ratio 1.0 and zero overlap. `len_bucket` is a cheap filter that removes definite non-matches;
`pos_bin` is the second filter, on position; and the real reciprocal-overlap predicate still runs on
whatever survives both. Neither bucket decides anything. Together they shrink the candidate set from
~506 billion pairs to ~15 million so the actual test becomes affordable.

**Base 2 is a choice, and it is already tight enough.** Any base works. Base 2 is the smallest whole-number
base whose band factor (2) exceeds the maximum ratio (1.25), and `LOG2` is a fast native function. A tighter
base — `FLOOR(LOG(len) / LOG(1.25))`, giving bands of exactly 1.25× — would prune slightly harder, but
measured bucket occupancy is already **2.49 intervals on average**, so there is essentially nothing left to
remove and the join runs in 486 ms. Not worth the complexity.

### B.6 What `pos_bin` prunes on, and the tolerance

`len_bucket` prunes on size. But two 1 000 bp deletions at opposite ends of chr1 have a perfect size ratio
and zero overlap, so a second filter is needed — on position. The question it must answer is: **how far
apart can two matching intervals start?**

Two intervals, A starting at `a` with length `L_A`, B at `b` with length `L_B`, and `Δ = b − a` with A
starting first:

```
        a                                        a+L_A
   A:   ├────────────────────────────────────────┤
        │
        │      b                            b+L_B
   B:   │      ├────────────────────────────────────┤
        │      │                                 │
        ├──Δ───┤                                 │
        wasted └────────── overlap ──────────────┘
```

The first `Δ` of A lies before B begins, so it can never be part of the overlap:

```
overlap ≤ L_A − Δ
```

Apply `overlap ≥ 0.8 × L_A`:

```
0.8 × L_A ≤ L_A − Δ        ⟹        Δ ≤ 0.2 × L_A
```

**The tolerance is 20% of the length of whichever interval starts *first*** — the one with the exposed
front end. Not the shorter one. That distinction is the subject of B.7 and is where a range join built on
the wrong bound lost 109 934 pairs (§5.2).

### B.7 Why the search radius is 0.25, not 0.2

Writing the search for a query interval `p`, we only know `L_p` — finding `t` is the whole point. So the
radius must be expressed in terms of `L_p`, and `t` may start on either side:

- **`p` starts first** — the wasted front is `p`'s, so `Δ ≤ 0.2 × L_p`. Already in terms of `L_p`. ✓
- **`t` starts first** — the wasted front is `t`'s, so `Δ ≤ 0.2 × L_t`. Not usable directly, but the length
  filter guarantees `L_t ≤ 1.25 × L_p`, giving `Δ ≤ 0.2 × 1.25 × L_p = 0.25 × L_p`.

The worse of the two governs, so the **safe search radius is `0.25 × L_p`**. In one sentence: *a partner
that is both longer and earlier can sit further away and still overlap enough, because it has more length
to spare.*

At the extreme, with `L_p = 1000` and a partner at the maximum allowed length starting before us:

```
   t = [0, 1250]      █████████████████████████
   p = [250, 1250]         ████████████████████
                      ├─Δ──┤
                        250
```

Overlap = 1000. Covers 80% of `p` (`1000 ≥ 800`) ✓ and 80% of `t` (`1000 ≥ 1000`) ✓ — a genuine match at
`Δ = 0.25 × L_p`. A radius of `0.2 × L_p = 200` misses it.

The reachable set is genuinely lopsided as a result — `p` reaches 250 bp left but only 200 bp right, since
"starts first" is directional. We search a symmetric `±0.25 · L_p` anyway; over-searching the right side by
50 bp costs a couple of candidates the exact test discards (B.11).

### B.8 The formula: zoom levels, not regions

```sql
pos_bin = FLOOR(start / 2^len_bucket)
```

`FLOOR(x / W)` chops the chromosome into cells of width `W` and reports which one you are in. Here
`W = 2^len_bucket`, so **the cell width is the base length of your size band**.

The mental model that fits is **map zoom levels**, not boxes inside boxes. `len_bucket` is not a region of
the genome — it is a size class, so it cannot "contain" positions. Instead there is one grid per size class,
all covering the same genome at different resolutions, and a point has a *different cell number at each
zoom*:

| level | cell width | our example's cell |
|---|---|---|
| bucket 16 | 65 536 bp | 933 |
| bucket 15 | 32 768 bp | 1867 |
| bucket 14 | 16 384 bp | 3735 |

Because the widths are powers of two and the grids share an origin, they nest exactly — each coarse cell is
precisely two finer ones. Running example: **chr11, start 61 203 531, length 45 643** (a real row from the
DRAGEN WES uplift output), which puts it in bucket 15 at `pos_bin 1867`:

```
                                                     ▼ 61 203 531
        ┌───────────────────────┬───────────────────────┬───────────────────────┐
b+1=16  │▓▓▓▓▓▓▓▓▓▓932▓▓▓▓▓▓▓▓▓▓│▓▓▓▓▓▓▓▓▓▓933▓▓▓▓▓▓▓▓▓▓│▓▓▓▓▓▓▓▓▓▓934▓▓▓▓▓▓▓▓▓▓│
        ├───────────┬───────────┼───────────┬───────────┼───────────┬───────────┤
b  =15  │   1864    │   1865    │▓▓▓1866▓▓▓▓│▓▓▓1867▓▓▓▓│▓▓▓1868▓▓▓▓│   1869    │
        ├─────┬─────┼─────┬─────┼─────┬─────┼─────┬─────┼─────┬─────┼─────┬─────┤
b-1=14  │ 3728│ 3729│ 3730│ 3731│ 3732│ 3733│▓3734│▓3735│▓3736│ 3737│ 3738│ 3739│
        └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘

        ▓ = one of the 9 probed cells        ▼ = where our interval starts
```

Cell 933 = 1866 + 1867 = 3732…3735. The three probed windows are **not** nested selections — each is
centred on where *we* fall at that zoom, computed independently. Reach shrinks as you zoom in: ~197 kb at
level 16, ~98 kb at 15, ~49 kb at 14. Coarser level, longer partners, larger tolerated displacement.

**The bins index start positions, not extents.** Our interval is 45 643 bp but its cell is only 32 768 —
it spans cells 1867, 1868 and into 1869, yet 1869 is never probed. That is correct: the join only ever asks
*"is the partner's **start** close enough to my **start**?"*. Where either interval ends is handled by the
length filter and the exact overlap test. This is why a 4 Mb interval works with the same nine probes
despite sprawling across hundreds of cells — only its first base pair is ever looked up.

This structure is the **UCSC binning scheme**, the same idea used for BAM and tabix indexing. It is the
standard answer to overlap queries on intervals spanning many orders of magnitude.

### B.9 Why `±1`, and the trap that comes with it

The key property, and the reason the width is tied to the length bucket:

```
tolerance = 0.25 × L_p     and     L_p < 2^(b+1) = 2W     ⟹     tolerance < 0.5 × W
```

**Every interval's search radius is under half its own cell width, automatically, at every scale.** Since
cell boundaries are `W` apart, a window of `0.5 W` can straddle at most one — so a partner is in the same
cell or an adjacent one, and `±1` suffices. Same number-line argument as B.4, with `W` in place of 1.

For our interval the radius is `±11 411`, i.e. `[61 192 120, 61 214 942]`, which crosses the boundary at
61 210 624 into cell 1868 — so a genuine partner can indeed sit one cell over.

**The trap, which has no analogue for `len_bucket`:** cell width differs per bucket, so bin numbers from
different buckets are not comparable. Same start, three contexts:

| looking in bucket | cell width | `pos_bin` |
|---|---|---|
| 14 | 16 384 | **3735** |
| 15 | 32 768 | **1867** |
| 16 | 65 536 | **933** |

Probing bucket 14 requires **3735**, not 1867 — hence `+ db` sitting *inside* the power in
`FLOOR(start / POW(2, len_bucket + db))`. Comparing 1867 against 3735 is comparing different scales; that is
exactly what `BETWEEN p.pos_bin - 1 AND p.pos_bin + 1` does, and it silently lost **861 196 pairs** (§5.2).

A corollary that looks like corruption if you read `pos_bin` as "where on the chromosome": two intervals
with the **same start** but different lengths get **different** `pos_bin`, because they are on different
maps.

Probing *down* a bucket is the tight case — the cells are half as wide but the tolerance is unchanged, so it
consumes nearly a whole cell. §5.3 has the table and the resulting `f ≥ 0.75` threshold floor.

### B.10 Why not a fixed-width grid

The obvious first design is a fixed cell width. It cannot work, because **the tolerance scales with length**
and CNV lengths in the DGV set span 1 bp to 4 123 510 bp:

| event length | tolerance (`0.25 × len`) | scaled cell (`2^len_bucket`) | tolerance ÷ cell |
|---|---|---|---|
| 300 bp | 75 bp | 256 | 0.29 |
| 1 000 bp | 250 bp | 512 | 0.49 |
| 45 643 bp | 11 411 bp | 32 768 | 0.35 |
| 4 123 510 bp | 1 030 878 bp | 2 097 152 | 0.49 |

The scaled cell is always 2–4× the tolerance. A fixed 100 kb cell is simultaneously **1 333× too wide** for
a 300 bp event (no pruning at all) and **10× too narrow** for a 4 Mb one (`±1` misses matches). Measured
probe radius a fixed 100 kb grid would need, per size class:

| len_bucket | intervals | max_len | probes **each side** |
|---|---|---|---|
| 5–17 | 597 927 | ≤ 262 143 | ±1 |
| 18 | 5 567 | 524 256 | **±2** |
| 19 | 2 289 | 1 046 884 | **±3** |
| 20 | 797 | 2 096 298 | **±6** |
| 21 | 242 | 4 123 510 | **±11** |

The radius stops being constant, so the clean three-value `CROSS JOIN` collapses. Measured pruning quality:

| scheme | bins | max k | avg k | Σk² |
|---|---|---|---|---|
| **scaled (`2^len_bucket`)** | 285 787 | 1 026 | 2.49 | **16 860 738** |
| fixed 100 kb | 168 689 | 1 128 | 4.22 | 24 247 166 |
| fixed 1 kb | 285 815 | **132** | 2.49 | **12 470 914** |

Fixed 100 kb is plainly worse. **Fixed 1 kb looks better** on Σk² — and that number is misleading. A finer
grid puts fewer intervals per cell but forces proportionally more cells to be probed for the same tolerance:
with 1 kb cells a 45 kb event needs `±17` instead of `±1`. Same candidates, ~33 M probe rows instead of
6.4 M. **Cell width only moves the bookkeeping; the candidate set is fixed by the geometry.**

**Measured end to end.** A fixed-width grid *can* be made correct on its own — drop `len_bucket` entirely
and prune on position alone — and it does work. Every variant below returns exactly 15 199 004 pairs, so
all are lossless; only the cost differs:

| join keys | probe cells | time | vs brute force |
|---|---|---|---|
| `chromosome` + `type` only (brute force) | 1 | 152 s | 1× |
| `len_bucket` only, ±1 | 3 | 40.8 s | 3.7× |
| `pos_bin` only, fixed 2 Mb, ±1 | 3 | 11.5 s | 13× |
| `pos_bin` only, fixed 512 kb, ±2 | 5 | 6.8 s | 22× |
| `pos_bin` only, fixed 128 kb, ±8 | 17 | **6.1 s** | 25× |
| **both keys, 9 cells** | 9 | **0.461 s** | **330×** |

Position is much the stronger of the two filters — positions spread over 3.1 Gb while lengths pile into a
few bands — but **position alone is still 13× slower than using both**, and length alone is 88× slower.

**The stronger objection to a fixed grid is the parameter, not the 13×.** The probe radius must be sized
from the *largest* interval in the dataset, so a single 4 Mb event dictates the radius for all 711 312:

| width | radius required | probe cells |
|---|---|---|
| 1 kb | **±1007** | 2015 |
| 8 kb | ±126 | 253 |
| 128 kb | ±8 | 17 |
| 2 Mb | ±1 | 3 |

At 1 kb that is ≈3.5 × 10¹⁰ comparisons — **worse than brute force**. The optimum near 128 kb is a property
of *this* length distribution and would need re-tuning for a different cohort, caller, or the arrival of
whole-chromosome events.

So the argument for scaling is not raw speed — it is **invariance**: a constant 9 probe keys at every scale,
one `CROSS JOIN`, no per-row radius, and no data-dependent constant to tune or get wrong.

### B.11 What the nine cells actually buy

Most of the nine are dead for any given interval, and that is fine.

For our 45 643 bp example, a partner's length must be in `[36 514, 57 054]` — a window that sits **entirely
inside bucket 15**:

```
   bucket 14              bucket 15                      bucket 16
├─────────────────┼───────────────────────────────┼──────────────────────►
16 384      32 767│32 768                   65 535│65 536         131 071
                  │      ├─────────────────┤      │
                  │   36 514           57 054     │
   ✗ too short         ✓ entirely inside            ✗ too long
```

So levels 14 and 16 cannot contribute. And of the three level-15 cells, the `±11 411` radius does not reach
back into 1866 — leaving **2 of the 9 live**. Confirmed by the data: this row's 36 real matches have
`min_match_len 36 516` and `max_match_len 55 790`, all inside bucket 15, with the minimum sitting almost
exactly on the theoretical floor of 36 514.

**All nine can never be live.** The partner-length window spans a factor of `1.25 / 0.8 = 1.5625`, narrower
than a band's factor of 2, so it can never reach across three bands — at least 3 of the 9 cells are
guaranteed waste for every interval. Neighbouring buckets only pay off near a band edge:

| our length | position in band | partner window | live buckets |
|---|---|---|---|
| 34 000 | near the bottom | 27 200 – 42 500 | 14 and 15 |
| **45 643** | middle | 36 514 – 57 054 | **15 only** |
| 60 000 | near the top | 48 000 – 75 000 | 15 and 16 |

Measured cost of that waste across the whole DGV set:

```
candidates examined (share a bin key) :  32 006 039
matches kept (pass 80% RO)            :  15 199 004
hit rate                              :  47.5%
```

Nearly half of everything the probe returns is a real match — a dead cell is a failed hash lookup, not a
scan. And where the matches come from:

| bucket offset | matches | share |
|---|---|---|
| **0** (same bucket) | 14 337 808 | **94.33%** |
| −1 | 430 598 | 2.83% |
| +1 | 430 598 | 2.83% |

The two neighbours contribute only 5.7% — but that is 861 196 pairs, exactly what the broken range join
lost, and they are the near-band-edge intervals the whole design exists to catch. (The two offsets being
*exactly* equal is a useful correctness signal: matching is symmetric, so every `+1` pair must have a `−1`
twin.)

Skipping the dead cells would require a per-row branch on band and cell edges, emitting a variable number of
keys, to shave a fraction of 486 ms. Not worth it.

**The general principle, which shows up three times in this design** — in the 0.25 radius, the `±1` cells,
and the nine probes:

> The filter must never lose a true match. It is allowed to be sloppy in the other direction, because the
> exact test cleans up after it.

Over-searching costs discarded candidates. Under-searching loses data silently and nobody finds out. This is
the standard **filter-and-refine** pattern — cheap uniform over-approximation, then the exact predicate on
the survivors.

---

## Appendix C — inside `probe` and `matches`

The two intermediate relations from §5.2. Neither is persisted in production — both are CTEs — but during
development and debugging it is worth materialising them, and worth knowing their exact shape. All sizes
below are measured on the DGV set (711 312 distinct intervals).

### C.1 `probe` — the 9-key explosion

**One row per (distinct interval × probe offset), so exactly 9 rows per interval.**

```
711 312 distinct intervals  ×  3 length buckets  ×  3 position cells  =  6 401 808 rows
```

Each row carries the interval's identity and geometry (`cnv_id`, `chromosome`, `type`, `start`, `end`) plus
the **target** key it is asking for (`t_len_bucket`, `t_pos_bin`). Only the target key changes across an
interval's 9 rows; the geometry is repeated, because the overlap predicate needs it after the join.

For the running example (chr11, `start 61 203 531`, `length 45 643`, so `len_bucket 15`, `pos_bin 1867`):

| `db` | `dp` | `t_len_bucket` | `t_pos_bin` |
|---|---|---|---|
| −1 | −1 | 14 | 3734 |
| −1 | 0 | 14 | **3735** |
| −1 | +1 | 14 | 3736 |
| 0 | −1 | 15 | 1866 |
| 0 | 0 | 15 | **1867** |
| 0 | +1 | 15 | 1868 |
| +1 | −1 | 16 | 932 |
| +1 | 0 | 16 | **933** |
| +1 | +1 | 16 | 934 |

These are the nine shaded cells in the B.8 diagram. Note the bolded `dp = 0` rows: **3735 / 1867 / 933** are
the same base pair expressed on three different grids (B.9).

**The nine target keys are pairwise distinct.** Rows with different `db` differ in `t_len_bucket`; rows with
the same `db` differ in `t_pos_bin`. Since any candidate row has exactly one `(len_bucket, pos_bin)`, it can
match **at most one** of an interval's nine keys — which is why no pair is produced twice and there is no
`DISTINCT` anywhere downstream.

### C.2 `matches` — the surviving pairs

**One row per ordered pair `(query_id, match_id)` that passes the overlap test.**

| | measured |
|---|---|
| candidates (share a target key, before the RO test) | 32 006 039 |
| **`matches` rows (after the RO test)** | **15 199 004** |
| hit rate | 47.5% |
| average matches per interval | 21.37 |
| maximum matches for one interval | 715 |

Two structural properties, both consequences of the predicate rather than of the implementation:

**Reflexive.** Every interval matches itself — for `a = b` the overlap is the full length, and
`len ≥ 0.8 × len` holds for any `len > 0`. The `db = 0, dp = 0` probe key always finds it. So `matches`
contains exactly **711 312 self-pairs**, one per distinct interval.

**Symmetric.** The predicate is symmetric in `a` and `b`, so `(X, Y)` appears if and only if `(Y, X)` does.
Both directions are stored deliberately: the frequency is *egocentric* (§3.3), so each interval needs its
own complete match set.

Together these give a cheap arithmetic invariant:

```
15 199 004  −  711 312 self-pairs  =  14 487 692 non-self ordered pairs
                                   =   7 243 846 unordered pairs   ← must be a whole number
```

**`(total − n_distinct_intervals)` must be even.** An odd result means a pair exists in one direction only,
which can only happen if the join is losing rows — exactly the failure mode of the broken range variant in
§5.2. It costs one query and catches a whole class of bug.

### C.3 Invariants worth asserting

| assertion | why it catches something |
|---|---|
| `probe` rows = `9 × COUNT(DISTINCT cnv_id)` | the `CROSS JOIN` degenerated, or a `NULL` length nulled a key |
| every `cnv_id` appears in `matches` at least once | reflexivity; a missing interval means a `NULL` `cnv_id` (§10.1) or a lost self-pair |
| `(matches − distinct intervals)` is even | asymmetry ⇒ the join is dropping rows |
| offsets `−1` and `+1` yield equal match counts | same, cross-bucket specific — measured 430 598 each |
| no pair appears twice in `matches` | a probe key collided; the nine should be pairwise distinct |

### C.4 Debugging queries

**Inspect one interval's nine probe keys.** Substitute the `cnv_id` under investigation:

```sql
SELECT d.len_bucket, d.pos_bin,
       d.len_bucket + b.db                                  AS t_len_bucket,
       FLOOR(d.`start` / POW(2, d.len_bucket + b.db)) + p.dp AS t_pos_bin
FROM distinct_cnv d
CROSS JOIN (SELECT -1 AS db UNION ALL SELECT 0 AS db UNION ALL SELECT 1 AS db) b
CROSS JOIN (SELECT -1 AS dp UNION ALL SELECT 0 AS dp UNION ALL SELECT 1 AS dp) p
WHERE d.cnv_id = :cnv_id
ORDER BY t_len_bucket, t_pos_bin;
```

Expect exactly 9 rows, all `(t_len_bucket, t_pos_bin)` distinct. If two coincide, the bin arithmetic is
wrong.

**Inspect one interval's matches with the actual overlap fraction**, to see how close each sits to the
threshold:

```sql
SELECT m.match_id, t.`start`, t.`end` - t.`start` AS match_len,
       ROUND((t.`end` - t.`start`) / (q.`end` - q.`start`), 3) AS len_ratio,
       ROUND(LEAST(
         GREATEST(0, LEAST(q.`end`,t.`end`) - GREATEST(q.`start`,t.`start`)) / (q.`end` - q.`start`),
         GREATEST(0, LEAST(q.`end`,t.`end`) - GREATEST(q.`start`,t.`start`)) / (t.`end` - t.`start`)
       ), 3) AS reciprocal_overlap
FROM matches m
JOIN distinct_cnv q ON q.cnv_id = m.query_id
JOIN distinct_cnv t ON t.cnv_id = m.match_id
WHERE m.query_id = :cnv_id
ORDER BY reciprocal_overlap DESC;
```

**`len_ratio` must lie in `[0.8, 1.25]` and `reciprocal_overlap` in `[0.8, 1.0]` for every row** — those are
forced by the predicate (§3.2, B.3). A value outside either range is a bug in the predicate, not a
biological finding. Same invariant as A.5, applied to a single interval.

### C.5 Whether to materialise them

In production both stay CTEs — the optimiser keeps the whole thing one statement and nothing needs
persisting. Materialise them when:

- **calibrating the threshold** (§5.5) — keep the raw overlap fraction in `matches` and cut it at several
  cutoffs without re-running the join;
- **comparing against brute force** (§6.3) — the set-difference check needs both sides as tables;
- **investigating a suspicious frequency** — C.4's queries are far easier against a persisted `matches`.

Sizes are modest: `probe` ≈ 6.4 M rows and `matches` ≈ 15.2 M rows of two `BIGINT`s. Both rebuild in
seconds.
