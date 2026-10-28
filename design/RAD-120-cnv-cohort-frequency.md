# RAD-120: In-cohort CNV frequencies

Spec (cross-module): radiant-hub `docs/features/RAD-120-cnv-cohort-frequency.md`. Pre-analysis, with the measures,
the full proof and the rejected alternatives: branch `docs/sjra-1786-cnv-freq`,
`design/SJRA-1786-cnv-cohort-frequencies.md`.

## Rule

An SNV has an exact key (`locus_id`), a CNV does not: two callers, or two samples, rarely report the same
breakpoints. Two CNVs **match** when:

- they are on the same chromosome;
- they are in the same class: `GAIN` and `GAINLOH` form one class (copy number up in both, same pooling as the
  gnomAD-SV join of the occurrence inserts), `LOSS` and `CNLOH` are each their own;
- their overlap covers at least **80% of each one's length** (reciprocal overlap).

The frequency is per distinct interval (`cnv_id`), not per cluster. The relation is not transitive (A matches B,
B matches C, A does not match C), so two overlapping intervals can carry slightly different frequencies. This is
expected. Clustering was rejected: single-linkage chaining can swallow a chromosome arm, and one new sample can
merge two clusters and change every frequency in them.

Exact-coordinate matching was measured on a 7 151-sample DRAGEN WES set: it labels about 4 CNVs per patient as
rare (< 1%) when they are in ≥ 5% of the cohort.

## Tables

Per tenant, one row per distinct `cnv_id` of the matching occurrence table:

| Table | Columns |
|---|---|
| `germline__cnv__variant_frequency` | `pc` / `pn` / `pf` × `wgs` / `wxs` × all / `_affected` / `_not_affected` (names of `germline__snv__variant_frequency`, no `hom`) |
| `somatic__cnv__variant_frequency` | `pc` / `pn` / `pf` × `_to_wgs` / `_to_wxs` |

- `pc`: patients with at least one `PASS` CNV that matches the interval. A patient with several matching segments
  counts once.
- `pn`: patients with at least one CNV occurrence of the cohort, whatever its filter.
- `pf = pc / pn`, NULL when `pn` is 0.
- Every interval has a row, also when none of its CNVs is `PASS` (`pc` = 0), as for the SNV frequencies since RAD-57.
- Somatic: tumor-only cohort only (1 tumoral aliquot, no normal, task verdict of the somatic SNV frequencies).
  All somatic CNVs come from tumor-only tasks today. The `_tn_` columns are added with `ALTER TABLE` when
  tumor-normal CNVs are ingested; until then a tumor-normal task belongs to no cohort.
- No `colocate_with`: the occurrence tables are not bucketed on `cnv_id`.

## Algorithm

One `INSERT OVERWRITE` per table and per tenant (`{germline,somatic}_cnv_variant_frequency_insert.sql`):

1. Collapse the occurrences to distinct intervals (`GROUP BY cnv_id`). `cnv_id` packs chromosome, start, end and
   type, so `ANY_VALUE` is safe.
2. Self-join the intervals on equality keys, with the overlap test as a residual:
   - `len_bucket = FLOOR(LOG2(end - start))`. 80% overlap of both bounds the length ratio to [0.8, 1.25], so the
     two buckets are equal or adjacent.
   - `pos_bin = FLOOR(start / 2^len_bucket)`. The start of a match is less than `0.25 × len` away, which is less
     than one bin of any of the three probed buckets.
   - Each interval probes 9 keys: bucket ±1, and the bin recomputed **with the target bucket's width**, ±1.
     Ranging on the probe's own bin compares different scales and silently loses the cross-bucket matches.
3. Join the matches to the `PASS` carriers and count distinct patients per cohort.
4. Denominators over all occurrences of the cohort; left join from the intervals so each one keeps a row.

The ±1 bins are lossless only for a threshold ≥ 0.75. Below it, matches are dropped with no error: widen to ±2
first. Checked against a brute-force join on 711 312 DGV intervals: same 15 199 004 pairs. A range predicate
instead of the 9 keys is a nested loop, 240× slower.

Measured: 5.4 s end to end on 5.76 M calls from 8 464 samples (DGV, the pessimistic case).

## Placement

Two tasks in `import_part`, after `insert_germline_cnv_occurrences` and `insert_somatic_cnv_occurrences`, mapped per
tenant like them and skipped with them when the part has no CNV task.

- A part cannot be counted alone: a patient of one part can match an interval of another. So there is no per-part
  staging table (unlike SNV), and each run recomputes the whole tenant.
- `import_part` runs one at a time (`max_active_runs=1` and the import mutex), so two recomputes never run at once.
  The pre-analysis placed it at the end of `import_radiant` because it assumed parallel parts.
- `reannotate_open_data` does not recompute: re-annotation does not change the CNV coordinates.

## Limits

- **Fragmented segments.** Segments are compared one by one; a caller event split into adjacent segments is not
  merged and undercounts. Measured on qlin prod (538 samples, 78 983 segments): 0.9% of segments are candidates
  (same class, same copy number, both `PASS`, less than 10 kb apart). If needed later: a gaps-and-islands merge
  as an extra CTE, matched with the same binned join.
- **WXS capture kits.** A WXS frequency is only meaningful across samples with the same targets. No split by kit.
- **Small cohorts.** `pf` on a small `pn` is an artifact filter, not a population frequency. Recurrent artifacts
  (centromeres, segmental duplications, noisy targets) show a high `pf`, which is what the table is for.

## Data QA

`radiant/data_qa/`: `cnv_id` not null and unique, `pf` in [0, 1], `pc ≤ pn`, and every occurrence `cnv_id` has a
frequency row (with the uniqueness: exactly one).
