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
`|start_a − start_b| ≤ 0.2 × min(len_a, len_b)`.

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

### 5.3 Why the binning is exact

The join is an equi-join on four columns with the overlap test as a cheap residual, which is what keeps
StarRocks from degrading to a nested loop. Both keys are derived from the geometry of the predicate, so
neither can drop a true match:

**Length.** `overlap ≥ 0.8·len_a` and `overlap ≤ min(len_a, len_b)` give `len_a/len_b ∈ [0.8, 1.25]`. Two
lengths within 1.25× of each other fall in the same `FLOOR(LOG2(len))` bucket or in adjacent ones, so
probing `±1` suffices.

**Position.** If `start_a < start_b` then `overlap ≤ end_a − start_b = len_a − |Δstart|`; combined with
`overlap ≥ 0.8·len_a` this gives `|Δstart| ≤ 0.2·len_a`, and symmetrically
`|Δstart| ≤ 0.2·min(len_a, len_b)`. With bin width `2^len_bucket` and `len < 2^(len_bucket+1)`, the maximum
displacement is `0.2 × 2^(tb+2) = 0.8 × 2^tb`, i.e. strictly less than one bin width — so `±1` bin suffices
for the target bucket and for both neighbours.

This is a proof, and it was also checked empirically against a full brute-force join (§6.3).

**The scheme is not tied to 0.8, but it does have a floor.** Generalising the two arguments to a threshold
`f`: `len_bucket ±1` is valid for any `f ≥ 0.25`, and `pos_bin ±1` for any `f ≥ 0.5`. So calibration (§5.5)
can move the cutoff anywhere in `[0.5, 1.0)` without touching the binning. **Below 0.5, `pos_bin` needs
`±2`** — otherwise matches are silently lost. Worth a comment in the SQL next to the constant.

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

1. **Calibrate once.** Keep the raw overlap fraction in `matches` and emit `pc` at 0.5 / 0.7 / 0.8 / 0.9 via
   conditional aggregation in one pass. Compare against gnomAD-SV AF and against the threshold-crossing
   counts from §7.1. Note the `f ≥ 0.5` floor from §5.3.
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

### 6.3 The binning was verified against brute force

Full O(n²) self-join on chr22 (15 288 distinct intervals) with the overlap predicate and **no bins at all**,
compared against the binned result:

| | pairs |
|---|---|
| brute force | 261 556 |
| binned | 261 556 |
| **missed by binning (false negatives)** | **0** |
| **extra from binning (false positives)** | **0** |

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

§5.5, with the `f ≥ 0.5` binning floor from §5.3. One-off exercise, needs real cohort data, should happen
before the table is exposed in the portal.

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
