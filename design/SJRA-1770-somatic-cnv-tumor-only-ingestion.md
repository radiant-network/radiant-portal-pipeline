# Somatic CNV (Tumor-Only) ingestion — work document

**Status:** draft for grooming. The data model is settled against two real DRAGEN files (§4); §9 lists what
is still open — coordination with other teams, not design choices. Not an implementation order.
**Related:** `SJRA-1751-somatic-snv-tumor-only-summary.md` (the TO decisions we inherit),
`SJRA-341-somatic-snv-ingestion.md` (germline-vs-somatic table split),
`SJRA-1751-snv-vcf-ingestion-fan-out.md` §4 (explicitly leaves the CNV gaps open)
**Reference implementation:** QLIN `clin-variant-etl` → `normalized/CNVSomaticTumorOnly.scala`,
`model/raw/VCF_CNV_Somatic_Input.scala`, `enriched/CNV.scala`

---

## 1. Goal

Ingest **somatic CNV for tumor-only analyses** so segments are queryable alongside germline CNV.
Shape it like germline CNV, informed by QLIN's `CNVSomaticTumorOnly`.

**In scope:** new Iceberg table, new StarRocks table, extraction, load, structural annotations.
**Out of scope:** cohort/population CNV frequencies of our own, tumor-normal CNV, API/UI.

The good news up front: **CNV is the simplest flow in the platform.** It writes exactly one table.
There is no variant catalogue, no consequence table, no shared-table race, and no frequency layer to
extend — the three things that made somatic SNV expensive. Almost all the work is mechanical
duplication of a path that already exists.

---

## 2. Summary — the decisions, and how we build it

**What we are adding:** one Iceberg table and one StarRocks table holding somatic tumor-only CNV segments,
loaded by the machinery germline CNV already uses. No frequencies of our own, no tumor-normal CNV.

### Major decisions

- **Discovery** — files are found by `task_type = tumor_only_variant_calling` + document
  `data_type = scnv`, reusing the existing `cnv_vcf_filepath` column. Both codes already exist in the
  clinical model, so **no clinical-model change**; a `WHERE` gate on `scnv` stops the calling task from also
  supplying the SNV path. (§3)
- **Tables** — `somatic_cnv_occurrence` (Iceberg, shared, partitioned by `tenant_code`) and
  `somatic__cnv__occurrence` (StarRocks, per-tenant). Two mapping-dict entries do most of the wiring. (§5)
- **Column set** — mirror germline, plus the DRAGEN 4.2.4 ASCN fields (`cn, cnf, cnq, mcn, mcnf, mcnq, maf,
  sd, as`), all nullable and often empty. `filter` stays `VARCHAR(255)` like every other occurrence
  table. (§5)
- **`type` comes from the DRAGEN ID** — `GAIN`, `LOSS`, `CNLOH`, `GAINLOH`. Measured: there is no usable
  copy-number fallback, since `CN`/`MCN` are absent or per-record and `SM` distributions overlap. A knowing
  coupling to DRAGEN's ID convention. (§4)
- **One row shape from either VCF version** — legacy 4.2 spells LOH as multi-allelic `<DEL>,<DUP>`, 4.4 as
  `<LOH>`; both normalise to a stored `alternate` of `<LOH>`, so the same event produces the same row
  either way. (§4)
- **`cnv_id` keys on `type`, not `alternate`** — this is what makes the two VCF versions interchangeable. It
  requires the shared `GET_CNV_ID` UDF widened to a 3-bit type field, taking its 2 extra bits from
  `start`. (§5)
- **gnomAD-SV joins on `type`** rather than `alternate`, so GAINLOH matches `DUP` and CNLOH correctly
  matches nothing. (§7)
- **Orchestration** — a second batch container beside germline CNV, same single-container shape.
  Deliberately *not* the SNV fan-out. (§6)

### How we implement it

1. **Discovery** — two lines in the staging view; new task-type constant and task model, with the
   tumor-only validation checks.
2. **Extraction** — new module under `radiant/tasks/vcf/cnv/somatic/`, mirroring germline, avoiding three
   measured traps: take `SVLEN[0]`, read ASCN fields defensively, skip records whose `type` will not resolve.
3. **Tables** — Iceberg init, the two mapping entries, the StarRocks DDL.
4. **Load SQL** — copy-partition and insert-delta, plus the gnomAD join change applied to germline too.
5. **Orchestration** — new TaskGroup in `import_part`, K8s and ECS operators, ECS entrypoint script.
6. **UDF** — cut the release, update both SQL call sites, backfill germline `cnv_id`s. **These three land
   together.**
7. **Tests, data QA and seeds.**

**The one change with reach beyond somatic CNV:** widening the shared UDF changes every existing germline
`cnv_id`. Nothing else here touches germline data.

---

## 3. How germline CNV finds its file today (answering "how is it calculated for germline")

> **TL;DR** — CNV files are never found by filename. They are found by **task type** + **document data
> type**. Germline is `alignment_germline_variant_calling` + `gcnv`; somatic tumor-only will be
> **`tumor_only_variant_calling` + `scnv`**, reusing the existing `cnv_vcf_filepath` column. Both codes
> already exist in the clinical model, so the change is two lines in one SQL view.

The rest of this section explains that mechanism, because it is where the somatic work actually starts
and none of it is obvious from the code that consumes it.

There is **no filename convention, no glob, no path template**. The pipeline never guesses where a
CNV VCF lives. It reads a URL out of the clinical catalogue. Four steps:

**Step 1 — the clinical catalogue says which document is a germline CNV VCF.**
A `task` row has a `task_type_code`. That task has output `document`s, each with a `format_code`
(`vcf`, `tsv`, `cram`…) and a `data_type_code` (`snv`, `ssnv`, `gcnv`, `scnv`…) and a `url`.
Germline CNV = the document whose task is `alignment_germline_variant_calling` and whose data type is
`gcnv`. The DRAGEN alignment-and-calling task emits the CNV VCF as one of its outputs.

**Step 2 — a StarRocks view pivots those documents into columns.**
`radiant/dags/sql/radiant/init/staging_external_sequencing_experiment_create_table.sql` — two places
matter, and they do different jobs:

```sql
-- line 53-55: which document lands in which column
ANY_VALUE(CASE WHEN d.format_code='vcf' AND d.data_type_code IN ('snv','ssnv') THEN d.url END) AS vcf_filepath,
ANY_VALUE(CASE WHEN d.format_code='vcf' AND d.data_type_code='gcnv'            THEN d.url END) AS cnv_vcf_filepath,
ANY_VALUE(CASE WHEN d.format_code='tsv'                                        THEN d.url END) AS exomiser_filepath,

-- line 65-69: which rows exist at all (a task type not listed here is invisible to the pipeline)
WHERE (
    (d.format_code='vcf' AND t.task_type_code IN ('radiant_germline_annotation','radiant_somatic_annotation'))
 OR (d.format_code='vcf' AND t.task_type_code = 'alignment_germline_variant_calling')
 OR (d.url LIKE '%variants.tsv' AND t.task_type_code = 'exomiser')
)
```

Note `vcf_filepath` is **already shared** between germline SNV (`snv`) and somatic SNV (`ssnv`) —
one column, disambiguated downstream by `task_type`. That is the precedent for CNV.

**Step 3 — the URL rides along the experiment row all the way to the task model.**
`cnv_vcf_filepath` is a plain column carried through `staging_sequencing_experiment`, the delta view,
`sequencing_experiment_insert.sql`, and finally `sequencing_experiment_partition_select.sql`, which is
the row shape `import_part` reads per partition.

**Step 4 — `task_type` picks the Python class, which picks the column.**
`radiant/tasks/vcf/experiment.py` maps `task_type` → a pydantic model. Germline CNV's model reads
exactly one field:

```python
ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK = "alignment_germline_variant_calling"

class AlignmentGermlineVariantCallingTask(BaseTask):
    task_type: str = ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK
    cnv_vcf_filepath: str

    @staticmethod
    def gather_additional_args(rows):
        if len(rows) > 1:
            raise ValueError("`alignment_germline_variant_calling` task expects a single row per task.")
        return {"cnv_vcf_filepath": rows[0]["cnv_vcf_filepath"]}
```

So: **`task_type` is the discriminator throughout, `data_type_code` only decides which column the URL
lands in.** Everything downstream (`sanity_check_cnvs`, `_TASK_TYPES` dispatch, `import_cnv_vcf`'s
own filter) branches on `task_type`, never on the file name.

### What this means for somatic — and the pleasant surprise

**The clinical vocabulary already exists.** No clinical-model change is needed.
`tests/resources/clinical/create_clinical_tables.sql` already seeds:

| Kind | Code | Label |
|---|---|---|
| `data_type` | **`scnv`** | Somatic CNV *(line 429)* |
| `task_type` | **`alignment_somatic_variant_calling`** | Genome Alignment and Somatic Variant Calling *(line 549)* |
| `task_type` | `somatic_variant_calling` | Somatic Variant Calling by Tumor-Normal Paired Samples |
| `task_type` | **`tumor_only_variant_calling`** | Somatic Variant Calling by Tumor-Only Sample |


**Decided: the task type is `tumor_only_variant_calling`.** So the pipeline-side change is two lines
in that one view:

```sql
-- add to the CNV CASE (line 54)
ANY_VALUE(CASE WHEN d.format_code='vcf' AND d.data_type_code IN ('gcnv','scnv') THEN d.url END) AS cnv_vcf_filepath,

-- add to the WHERE (line 65-69)
 OR (d.format_code='vcf' AND d.data_type_code='scnv' AND t.task_type_code='tumor_only_variant_calling')
```

The `data_type_code='scnv'` gate is deliberate: `tumor_only_variant_calling` is a *calling* step, so it
may also carry a raw, un-annotated SNV VCF, and without the gate that file could silently land in
`vcf_filepath` — which must always hold the annotated SNV from `radiant_somatic_annotation`. Germline's
line has no such gate and gets away with it only because that step registers nothing but the `gcnv` file;
don't copy the looser version.

The code exists in the `task_type` dictionary (line 552) but **no `task` row uses it yet** — nothing in
`seeds.sql` references it, so seeds are genuinely new work, not a copy-edit.

**Recommendation: reuse the existing `cnv_vcf_filepath` column** rather than adding a
`somatic_cnv_vcf_filepath` — exactly as `vcf_filepath` already serves both SNV flows. A task is either
germline-calling or tumor-only-calling, never both, so there is no collision, and a new column would
otherwise ripple through five SQL files plus the partition assigner.

---

## 4. What the input actually looks like

Illumina **DRAGEN somatic tumor-only CNV**. QLIN's config pins the file shape:
`*.dragen.WES_somatic-tumor_only.cnv.vcf.gz` (germline is `*[0-9].cnv.vcf.gz`).

Two real files from the cohort, and the DRAGEN versions differ materially.

**A — `21305.…cnv.vcf.gz`, DRAGEN 3.10.8, VCF 4.2, 304 records, non-ASCN, no LOH:**

```
chr1  450730    DRAGEN:LOSS:chr1:450731-7249626     N  <DEL>  128  cnvCopyRatio;LoDFail
                SVLEN=-6798896;SVTYPE=CNV;END=7249626;REFLEN=6798896  GT:SM:BC:PE  0/1:1.04404:1979:25,7
```

**B — `25341.…`, DRAGEN 4.2.4, ASCN fields declared, LOH present:**

```
chr1  6667699   DRAGEN:CNLOH:chr1:6667700-6887749   N  <DEL>,<DUP>  3  cnvQual;segmentMean
                SVLEN=-220050,220050;SVTYPE=CNV;END=6887749;REFLEN=220050
                GT:SM:SD:MAF:BC:AS:PE   1/2:1.018696:702.9:0:20:1:4,0
```

| | **A — 3.10.8** | **B — 4.2.4** |
|---|---|---|
| LOH records | none | **yes** — `DRAGEN:CNLOH:…`, ALT `<DEL>,<DUP>` |
| FORMAT declared | `GT SM BC PE` | `AS BC CN CNF CNQ GT MAF MCN MCNF MCNQ PE SD SM` |
| FORMAT on the LOH row | — | `GT SM SD MAF BC AS PE` — **`CN`/`MCN` absent despite being declared** |
| `CIPOS` / `CIEND` | declared, never populated | **not declared at all** |
| INFO extras | — | `HET` flag ("segment is heterogeneous") |
| `FILTER` vocabulary | `cnvQual cnvBinSupportRatio cnvCopyRatio LoDFail` | `cnvQual cnvLength binCount segmentMean` |
| `INFO/END` | all 304, `END == POS+REFLEN` | same — 6887749−6667699 = 220050 = REFLEN |
| `type` × ALT (file A) | GAIN↔`<DUP>` 153, LOSS↔`<DEL>` 151, no `.` | — |
| `FILTER` multiplicity (A) | 22% carry two (`cnvCopyRatio;LoDFail` ×68) | LOH row also multi: `cnvQual;segmentMean` |
| max start / length (A) | 228,007,104 / 92,449,127 — inside the 28-bit cap ✓ | contigs confirm chr1 = 248,956,422 |

**Five things these establish:**

1. **LOH is multi-allelic `<DEL>,<DUP>` in *both* versions — `<LOH>` has never been seen.** Neither header
   declares an `LOH` ALT. So the VCF 4.4 `<LOH>` spelling is forward-looking only: everything in the cohort
   today uses the legacy pair. We still *store* `<LOH>` as the canonical value, but the input branch that
   recognises it is untested until a 4.4 file appears.
2. **⚠ `SVLEN` carries one value per ALT allele on LOH rows** — `SVLEN=-220050,220050`, declared `Number=.`.
   Germline does `record.INFO.get("SVLEN")`, which returns a **tuple** for a multi-value INFO, against a
   scalar `IntegerType` field. That breaks on every LOH row. Take element 0, as QLIN does with
   `INFO_SVLEN(0)`.
3. **`CN`/`MCN` are per-record, not per-file.** 4.2.4 declares them, yet the LOH row omits them from its own
   FORMAT. They must be nullable and read defensively — germline's `if record.format("CN") is not None`
   pattern is correct. It also re-confirms that the LOH split cannot depend on them.
4. **`MAF=0` is the direct LOH marker** where present ("MAP estimate of minor allele frequency"), and worth
   capturing as a column — but per-record availability means it cannot be the primary source either.
5. **`GT` is `1/2` on LOH rows**, referencing both alt alleles, so `calls` becomes `[1,2]` rather than
   germline's `[0,1]`. Harmless for an `ARRAY<INT>` column, but surprising if unexpected.

Two smaller notes:

- **`INFO/END` is reliable, so `record.end` is correct** — no need for QLIN's `POS + REFLEN + 1`, which is
  not a discrepancy either: glow's `start` is 0-based, so it lands on `END` exactly. Do not "fix" either one.
- **The `FILTER` vocabularies are disjoint between versions**, so the QA accepted-values list must be a
  union: `PASS, cnvQual, cnvLength, binCount, segmentMean, cnvBinSupportRatio, cnvCopyRatio, LoDFail`.

Three differences from the germline CNV file that matter, all coming from DRAGEN 4.x / VCF 4.4:

1. **`type` comes from the ID column, not ALT.** QLIN uses `split(name, ":")(1)` on
   `DRAGEN:LOSS:chr1:9823628-9823687`, and its QC dictionary lists the domain as
   `GAIN, LOSS, GAINLOH, CNLOH` (plus `null`). Our germline code instead reads ALT
   (`<DUP>` → GAIN, `<DEL>` → LOSS, else `UNKNOWN`), which cannot express the two LOH types at all.
2. **ALT can be `.`** for reference / non-variant segments. QLIN drops those rows early:
   `.where(size($"alternateAlleles") > 0 && $"alternateAlleles"(0) =!= ".")`.
3. **`SVTYPE` may be absent** in VCF 4.4; QLIN defaults it to `"CNV"`.

### How DRAGEN actually represents LOH — *resolved, and better than feared*

`.` marks **REF calls only**; LOH gets its own representation — but *which* one depends on a DRAGEN flag,
and **we need to support both**:

| Event | ID column | ALT (VCF 4.4) | ALT (VCF 4.2, `--cnv-enable-legacy-vcf-format`) |
|---|---|---|---|
| gain / loss | `DRAGEN:GAIN:…` / `DRAGEN:LOSS:…` | `<DUP>` / `<DEL>` | same |
| CNLOH / GAINLOH | `DRAGEN:CNLOH:…` / `DRAGEN:GAINLOH:…` | **`<LOH>`** | **`<DEL>,<DUP>`** (multi-allelic) |
| reference segment | `DRAGEN:REF:…` | `.` | same |

So QLIN's ALT=`.` filter drops REF segments and *keeps* LOH — its CNLOH support is real, not accidental.
The tension flagged in an earlier draft does not exist.

**QLIN production confirms the legacy form.** Their counts pair up exactly — `CNLOH <DUP>` 149,745 /
`CNLOH <DEL>` 149,745, `GAINLOH <DEL>` 52,893 / `GAINLOH <DUP>` 52,893 — and every LOH row has
`is_multi_allelic = true`. Their reader (glow, `split=true`) splits each `<DEL>,<DUP>` record into two rows,
so an LOH event is stored twice, once per haplotype.

**We do not inherit that doubling.** cyvcf2 does not split multi-allelic records: `record.ALT` is simply
`['<DEL>','<DUP>']` on a single record. So we get **one row per event in both formats** for free — but note
our LOH row counts will therefore be half of QLIN's.

Also worth knowing: **LOH types exist only in DRAGEN's ASCN workflow** (allele-specific copy number). If it
is not enabled upstream, there are no LOH records to ingest at all.

### Decided normalisation — one shape from either format

The two formats spell an LOH event differently, so we normalise **the legacy spelling onto the current
one** — 4.2's `<DEL>,<DUP>` becomes `<LOH>`, which is what 4.4 already emits for the same event. Nothing is
invented, and both formats then produce identical rows:

#### Resolving `type` — the ID is the only reliable source

1. **Read the type off the ID.** DRAGEN writes `DRAGEN:<TYPE>:<chr>:<start>-<end>`, so `GAIN`, `LOSS`,
   `CNLOH` or `GAINLOH` comes straight from it. **This is the primary and, in practice, the only path** —
   see the measurement below.
2. **If the ID is unparseable, ALT still settles the non-LOH cases:**

   | ALT | `type` |
   |---|---|
   | `<DUP>` | `GAIN` |
   | `<DEL>` | `LOSS` |
   | `.` | REF segment — skip the record |

3. **An LOH event with no parseable ID cannot be classified — fail loudly.** Two ALTs (4.2) or `<LOH>`
   (4.4) tells us it *is* LOH, but nothing available distinguishes `CNLOH` from `GAINLOH`. Do not guess.
4. **Never write an `UNKNOWN` row.** Its `cnv_id` would be NULL against a `NOT NULL` key column and the
   whole load would fail (§5). Skip, or fail, but do not persist it.

**Why there is no copy-number fallback.** An earlier draft of this document proposed deriving the LOH split
from `CN`, or from `MCN=0`. Measured against a real DRAGEN somatic tumor-only file, that is not available:

- **`CN` and `MCN` cannot be relied on.** DRAGEN 3.10.8 does not emit them at all (FORMAT is
  `GT:SM:BC:PE`); 4.2.4 declares them but omits them from the LOH row's own FORMAT. This is also why
  QLIN's schema has no `cn` — accurate to the files, not an oversight as an earlier draft assumed.
- **`SM` (segment-mean copy ratio) is always present but not separable.** Over file A's 304 records: GAIN
  spans 1.003–1.832, LOSS spans 0.035–**1.074** — so **25 of 151 losses sit above 1.0**. A threshold there
  misclassifies 16% of them, and the overlap is intrinsic (those rows carry the `cnvCopyRatio` filter,
  "copy ratio within ±0.2 of 1.0"). File B's CNLOH row sits at SM 1.019, i.e. inside that same band.

So relying on the DRAGEN ID is not a stylistic preference — it is forced by the data. The portability
concern about other callers stands, but the honest limit is: a caller without a parseable ID gives us
GAIN/LOSS from ALT and **no LOH classification at all**.

#### What ends up stored

```
type      ← the rule above
alternate ← <DUP> | <DEL> as-is;  both LOH forms normalised to <LOH>
cnv_id    ← GET_CNV_ID(chromosome, start, length, type)      -- type, not alternate
```

| `type` | ALT in 4.2 | ALT in 4.4 | **stored `alternate`** | gnomAD-SV join looks for |
|---|---|---|---|---|
| GAIN | `<DUP>` | `<DUP>` | `<DUP>` | DUP |
| LOSS | `<DEL>` | `<DEL>` | `<DEL>` | DEL |
| GAINLOH | `<DEL>,<DUP>` | `<LOH>` | `<LOH>` | DUP |
| CNLOH | `<DEL>,<DUP>` | `<LOH>` | `<LOH>` | *no match* |

Note the last column is a **join key only** — it is never stored. A GAINLOH row reads
`type=GAINLOH, alternate=<LOH>`; nothing in the table calls it a duplication. See §7 for the join itself.

Four properties worth calling out:

- **The ID column is a convenience, not a dependency.** LOH is detectable without it in both formats — two
  ALTs in 4.2, `<LOH>` in 4.4 — and `CN` vs expected ploidy (or `MCN=0` at normal `CN`, the direct
  minor-haplotype signal) separates CNLOH from GAINLOH. So a caller that leaves `ID` empty still resolves,
  which answers the portability concern about relying on `DRAGEN:<TYPE>:…`.
- **Collapsing the legacy pair is lossless.** The two ALTs are not two facts; they are one event stated as
  "one haplotype down, one up". The allele-specific detail lives in `CN`/`MCN`, which we keep — so
  `<LOH>` + `CN`/`MCN` carries everything `<DEL>,<DUP>` did.
- **`alternate` stays a scalar `varchar` with three values.** Considered and rejected: keeping the raw
  string (`<DEL>,<DUP>`), which would make the column format-dependent — the same event spelled differently
  depending on an upstream flag — and a permanent trap for anyone writing a filter, facet or QA rule
  against it. If provenance of the source format is ever needed, that belongs in a provenance field or the
  task's pipeline version, not overloaded into `alternate`. An array was also rejected: it would store a
  constant, since legacy LOH is *always* exactly `<DEL>,<DUP>`.
- **The same event gets the same `cnv_id` under either format**, which an ALT-based id could not deliver:
  4.2 and 4.4 would have disagreed on the ALT and so on the id.

**The residual risk of depending on the ID.** The VCF spec treats it as **optional** (`.` when absent), and
for symbolic alleles it is the **ALT** that carries the event type — the ID carries an identifier, strictly
needed only to link breakend mates via `MATEID`. `DRAGEN:<TYPE>:<chr>:<start>-<end>` is a vendor convention,
not a standard; no documentation shows GATK gCNV, CNVkit or Canvas populating ID with a parseable type. We
depend on it anyway because, as measured above, nothing else in the file can classify LOH. Worth recording
as a known coupling to DRAGEN rather than pretending we have a fallback.

---

## 5. New tables

Two new tables, following the naming conventions exactly:

| Layer | Name | Notes |
|---|---|---|
| Iceberg | `somatic_cnv_occurrence` | shared across tenants, partitioned by `tenant_code` (as germline CNV) |
| StarRocks | `somatic__cnv__occurrence` | **per-tenant** — lives in `{tenant}_tenant` DB, no `tenant_code` column |

Registering them is nearly free, thanks to two existing conventions:

- adding `"iceberg_somatic_cnv_occurrence"` to `ICEBERG_RADIANT_MAPPING` also gets the table into
  `refresh_iceberg_tables` automatically;
- adding `"starrocks_somatic_cnv_occurrence"` to `STARROCKS_RADIANT_PER_TENANT_MAPPING` makes
  `prepare_tenants_tables` (in `import_radiant.py`) create it for every tenant, provided a file named
  `sql/radiant/init/somatic_cnv_occurrence_create_table.sql` exists. **The filename must match the
  mapping key minus the `starrocks_` prefix** — that is the whole contract.

### The `cnv_id` UDF is what really constrains the LOH decision

`GET_CNV_ID(chromosome, start, length, alternate)` can be reused for `<DUP>`/`<DEL>` — but it cannot
express LOH types, and it does not fail softly.
[`CNVIdUDF.java`](https://github.com/radiant-network/radiant-starrocks-udf/blob/main/src/main/java/org/radiant/CNVIdUDF.java)
is a bit-packed encoding, not a hash:

```
encoded = (isGain << 63) | (chromNum << 58) | (start << 28) | length
            1 bit           5 bits            30 bits         28 bits     = 64 bits, fully consumed
```

Two consequences:

1. **`alternate` is the type discriminator, and it has exactly one bit.** `<DEL>` → 1, `<DUP>` → 0.
   There is no room for a third or fourth value: `start` needs its 30 bits (max 999,000,000 < 2³⁰) and
   `length` needs its 28 (chr1 is 248.9 Mb < 2²⁸). Nothing is spare. Supporting four types means a
   **2-bit type field, i.e. re-cutting the layout** — not a parameter addition.
2. **Any other ALT returns NULL.** And `cnv_id` is declared `bigint(20) NOT NULL` and sits in
   `DUPLICATE KEY(part, seq_id, task_id, cnv_id)`, with a `not_null` QA test on top. So a record whose
   ALT is `<CNV>` or `.` does not degrade to a `UNKNOWN` row — **it fails the StarRocks load.**

**Note this is already a latent landmine in germline.** The germline CNV VCF header declares
`##ALT=<ID=CNV,…>` alongside DEL and DUP, and the germline extractor skips only records with *no* ALT
(`if not record.ALT`) — a `<CNV>` record sails through with `type='UNKNOWN'` and then produces a NULL
`cnv_id`. It has evidently never occurred in practice, but nothing prevents it.

**Decided: widen the type field in the shared UDF** (one function for germline and somatic, not a
somatic-only variant).

**Agreed layout — the 2 extra bits come from `start`:**

```
bits 61-63  type     3 bits   8 values          (was 1 bit, 2 values)
bits 56-60  chrom    5 bits   25 used           (unchanged)
bits 28-55  start   28 bits   max 268,435,455   (was 30 bits, guard 999,000,000)
bits  0-27  length  28 bits   max 268,435,455   (unchanged)
```

Why `start` and not the others:

- **`chromosome` cannot shrink** — 25 values are needed (1–22, X, Y, M); 4 bits gives 16.
- **`length` cannot shrink.** At 27 bits the ceiling is 134,217,727 (~134 Mb), which would truncate
  whole-chromosome events on chr1–9, chr11 and chrX, plus arm-level events on chr2 q (146,193,529) and
  chr4 q (138,414,555) — precisely the aneuploidy and arm-level events somatic CNV is full of. Worse, the
  boundary falls mid-karyotype: chr11 (135,086,622) overflows while chr10 (133,797,422) and chr12
  (133,275,309) fit, so a bug here would pass or fail depending on which chromosome was tested.
- **`start` had genuine slack.** It is bounded by the same number as `length` — the largest chromosome
  position, chr1's 248,956,422 — so 28 bits is its natural size too; the old 30 bits and the 999,000,000
  guard were 4× any real human coordinate. The remaining 7.8% headroom is against a fixed biological
  constant that does not grow (T2T-CHM13's chr1 is 248,387,328, slightly smaller), and it is the same
  margin `length` has run at in production all along.

Taking 2 bits rather than 1 costs nothing extra — the migration is the same either way — and 8 type values
means never re-cutting this layout.

Implementation note: assign the four known types the values **0–3**, which leaves bit 63 clear and so keeps
every `cnv_id` positive; only future types 4–7 would set the sign bit. (Today `<DEL>` sets bit 63, so
existing germline LOSS ids are negative — another thing the backfill normalises.)

**Also pass `type` rather than `alternate`** — this is what makes supporting both VCF versions possible at
all. The two formats spell an LOH event differently (`<DEL>,<DUP>` vs `<LOH>`), so an ALT-keyed id would
give the same biological event two different `cnv_id`s depending on an upstream flag. `type` is identical
under both (§4).

**Consequence: every existing germline `cnv_id` changes** — both because the layout moves and because the
sign convention does. That means a germline CNV backfill (mechanical: the StarRocks table is rebuilt per
part by partition swap) and any externally stored or bookmarked `cnv_id` goes stale. Sequence the UDF
release, the SQL change and the backfill together.

**Keep an ALT guard in the extractor regardless.** Even with a widened UDF, an ALT the mapping does not
recognise still produces a NULL `cnv_id` and a failed load, so unknown ALTs should be skipped explicitly
instead of reaching StarRocks. This also closes the germline landmine above.


### Column set — decided: option C, plus `mcn`

Three shapes were considered. All produce a working table; they differ in how far they diverge from
germline and how much they preserve. Common core, identical in all three:

> `part`, `seq_id`, `tenant_code`, `task_id`, `aliquot`, `chromosome`, `alternate`, `start`, `end`,
> `type`, `length`, `name`, `quality`, `calls`, `bc`, `pe`, `sm`, `svtype`, `svlen`, `reflen`, `phased`

The differences:

| | **A — QLIN somatic shape** | **B — mirror germline exactly** | **C — germline columns + somatic type parsing** |
|---|---|---|---|
| `cn` (FORMAT CN) | dropped | kept | kept, nullable — **absent from the measured file** |
| `cipos` / `ciend` | dropped | kept | kept, nullable — **declared but never populated** |
| `filter` | `filters ARRAY<VARCHAR>` (split on `;`) | `filter VARCHAR` (single) | `filter VARCHAR(255)` — consistent with every other occurrence table |
| `type` source | ID column → `GAIN/LOSS/CNLOH/GAINLOH` | ALT → `GAIN/LOSS/UNKNOWN` | ID, ALT for non-LOH (§4) |
| CNLOH / GAINLOH | preserved¹ | **collapse to `UNKNOWN`** | preserved¹ |
| Cost | new parsing + a divergent shape | least new code | small extra parsing |

¹ Recording the type is the easy half; making the row loadable is the other half — per §4, `cnv_id` is keyed
on `type`, which needs the widened UDF below.

**Why C — one CNV shape across germline and somatic.** The two StarRocks tables stay comparable and the DDL
and QA files stay near-copies. `cn`, `cipos` and `ciend` are kept **nullable**, which costs nothing, whereas
dropping them would need a migration to undo.

Be aware they will be empty or near-empty: 3.10.8 has no `CN` at all and declares `CIPOS`/`CIEND` without
populating them, while **4.2.4 does not declare `CIPOS`/`CIEND` at all**. So all three go on the QA
`should_not_contain_only_null` except-list, exactly as germline already exempts `svlen`/`cipos`/`ciend` as
"constant by construction" — and `cipos`/`ciend` are arguably worth dropping outright, since no observed
version populates them. A correction to an earlier draft: QLIN dropping `cn` was **accurate to the files**,
not the oversight that draft assumed.

**Add the 4.2.4 ASCN fields, all nullable.** File B declares `CN, CNF, CNQ, MCN, MCNF, MCNQ, MAF, SD, AS`
(plus an INFO `HET` flag) — none of which exist in 3.10.8 output, and several of which are missing even from
file B's own LOH row. So capture them, expect NULL widely, and put them on the QA except-list. `maf` is the
most valuable of them: `MAF=0` is the direct LOH marker. None may be *depended* on — per §4 the LOH split
comes from the ID, because no copy-number field is reliably available.

**`filter` stays a scalar `VARCHAR(255)`.** Every occurrence table in the platform declares it that way —
`germline__cnv__occurrence`, `germline__snv__occurrence` and `somatic__snv__occurrence` alike — and CNV should
not be the one exception. So multi-filter rows are stored as DRAGEN writes them, semicolon-joined
(`cnvCopyRatio;LoDFail`), which is 22% of the records in file A.

Two consequences for the QA file, not for the schema: *"has `LoDFail`"* is a `LIKE` rather than an
`array_contains`, and a plain `accepted_values` test on `filter` would have to enumerate *combinations*
rather than values — so use a split-and-check custom test, as germline effectively does by not testing
`filter` at all. The vocabulary itself is version-dependent and must be a union (§4).

Still to confirm (§9 Q1): whether an **LOH-bearing** file carries `CN`/`MCN`. The measured file has no LOH, so
it cannot answer that.

---

## 6. Extraction and orchestration — *decided: mirror the germline pattern*

Add a second batch container alongside the germline one, same shape:

```
radiant-import-part
  vcf_imports = [ import_snv_vcf (TriggerDagRun → the fan-out sub-DAG)
                  import_cnv_vcf           ← germline, unmapped, loops all tasks in the part
                  import_somatic_cnv_vcf   ← new, same shape ]
```

New module `radiant/tasks/vcf/cnv/somatic/{occurrence,process}.py`, mirroring
`cnv/germline/`; new `ImportPart.get_import_somatic_cnv_vcf` in both `operators/k8s.py` and
`operators/ecs.py`; new `scripts/ecs/import_somatic_cnv_vcf.py`. The K8s CNV resource profile
(1 cpu / 500Mi / 1Gi limit) should apply unchanged — a tumor-only CNV file is single-sample and
segment-level, so it is far smaller than the SNV files that profile was sized against.

**Deliberately not doing:** restructuring CNV into a fanned-out sub-DAG the way SNV was in SJRA-1751.
That ticket left CNV's two known gaps open on purpose (it feeds the raw, unfiltered task list, and it
tolerates failed downloads by skipping — which per SJRA-1187 makes a missing sample *permanently*
invisible, because the experiment still gets marked ingested). Both gaps will be duplicated into the
somatic path by this choice. **That is accepted here and should stay tracked as its own ticket**,
which then fixes both flows at once rather than only the new one.

---

## 7. Load and annotations — *decided: structural + keep gnomAD-SV*

Two new SQL templates, near-copies of the germline pair:

- `somatic_cnv_occurrence_copy_partition.sql` — the 3-line survivor copy, verbatim
- `somatic_cnv_occurrence_insert_partition_delta.sql` — the enrichment query

Wired into `import_part.py` as a new TaskGroup mirroring `germline_cnv_occurrence`: a
`sanity_check_somatic_cnvs` short-circuit on the new `task_type`, then a
`RadiantStarRocksPartitionSwapOperator.partial(...).expand_kwargs(tenant_params)`. It can sit
immediately after the germline CNV group in Phase 3.

The enrichment carries over, with **one substantive change**:

| Annotation | Somatic |
|---|---|
| `cytoband` | same — join `starrocks_cytoband` on overlap |
| `symbol` / `nb_genes` | same — join `starrocks_ensembl_gene` on overlap |
| `nb_snv` | **must join `iceberg_somatic_snv_occurrence`, not the germline one** |
| `gnomad_af/sc/sn/sf/sc_hom/sc_het` | **kept**, same 80% reciprocal-overlap logic, but the join moves off `alternate` — see below |

`nb_snv` is the one that would be quietly wrong if copied verbatim: counting germline SNVs inside a
somatic segment is meaningless. QLIN makes the same distinction, keying its SNV count on
`bioinfo_analysis_code` so somatic CNV only counts somatic SNVs. Note this creates an **ordering
constraint**: the somatic CNV load must run after somatic SNV occurrences exist for the part.
Germline CNV already sits before `insert_variant_hashes` for the mirror-image reason — worth checking
whether the somatic CNV group has to move later in Phase 3/4 rather than sitting next to germline's.

### The gnomAD-SV join keys on `type`, not `alternate`

gnomAD stays because it is *population* data, not a cohort frequency of ours. But the germline join
condition cannot be reused verbatim:

```sql
-- today (germline): only works because `alternate` happens to be <DUP>/<DEL>
ON cnv.chromosome = gnomad.chromosome AND cnv.alternate = gnomad.alternate
   ... AND gnomad.svtype IN ('DUP','DEL')

-- replacement: say what the join actually means — same copy-number direction
ON cnv.chromosome = gnomad.chromosome
AND gnomad.svtype = CASE WHEN cnv.type IN ('GAIN','GAINLOH') THEN 'DUP'
                         WHEN cnv.type = 'LOSS'             THEN 'DEL' END
   ... /* reciprocal 80% overlap and gnomad.filters='PASS' unchanged */
```

- The separate `gnomad.svtype IN ('DUP','DEL')` filter **disappears** — the `CASE` yields only those two or
  NULL, and NULL never equals anything. Net simpler than today.
- **CNLOH matches nothing**, correctly: copy-neutral, so there is no copy-number change to compare against
  a duplication/deletion frequency. Its `gnomad_*` columns stay NULL via the existing `LEFT JOIN`.
- **GAINLOH matches DUP**, because it *is* a copy gain, so "how often is this region duplicated in the
  population?" is a meaningful benign-region signal. This is a join key only — the row still stores
  `alternate=<LOH>` (§4). *(The conservative alternative is to drop `'GAINLOH'` from the `CASE` and give it
  no annotation; that changes no stored column.)*
- **Apply the same change to germline** and the two SQL files stay near-copies. It is behaviour-preserving
  there: germline's `type` is *derived from* `alternate`, so the two are perfectly correlated and the `CASE`
  reproduces today's matches exactly, with `UNKNOWN` → NULL just as an odd ALT fails to match today.

Standing caveat, unchanged: gnomAD-SV is a **germline** reference, so an overlap means "this region is also
polymorphic in the general population", not "this somatic event is common".

---

## 8. Work breakdown

Roughly in dependency order. Nothing here is large; the count is the cost, not any single item.

1. **Discovery** — `staging_external_sequencing_experiment_create_table.sql`: add `'scnv'` to the CNV
   `CASE`, and add `tumor_only_variant_calling` to the `WHERE` **gated on `d.data_type_code='scnv'`** so
   the calling task can never supply `vcf_filepath` (§3). No other SQL changes, since we reuse
   `cnv_vcf_filepath`. Worth an integration assertion that a `tumor_only_variant_calling` row has
   `vcf_filepath IS NULL` and `cnv_vcf_filepath IS NOT NULL`.
2. **Task model** — `TUMOR_ONLY_VARIANT_CALLING_TASK = "tumor_only_variant_calling"` + a `BaseTask`
   subclass + `_TASK_TYPES` entry in `radiant/tasks/vcf/experiment.py`. The task type asserts tumor-only,
   but validate the file against it — three cheap checks: exactly one experiment on the task; that
   experiment's `histology_type == 'tumoral'` (stops a mislabelled normal-only task writing normal depths
   as tumor values); and the VCF declaring exactly one sample. For the third, capture `vcf.samples`
   **before** `set_samples` narrows it — the trick from `snv/somatic/process.py:44-49`, since narrowing
   rewrites `vcf.samples` and `vcf.raw_header` and destroys the evidence. It is the only check that catches
   a genuinely tumor-normal file mislabelled upstream; since TN CNV is out of scope, fail loudly.
   Minor: prefer `cnv_vcf_filepath: str | None = None` over germline's required `str`. The `scnv` gate
   makes a missing path impossible, so this is only about failing small if that ever changes — a required
   field raises while the whole partition's task list is being built, so it fails every experiment in the
   partition rather than skipping one file.
3. **Extraction** — `radiant/tasks/vcf/cnv/somatic/{__init__,occurrence,process}.py`: pyiceberg
   `SCHEMA` + `process_occurrence` + `process_tasks` + `import_somatic_cnv_vcf`. Use a distinct
   Iceberg field-ID range (germline CNV occupies 100-124 / 200-203). **While here, confirm the DRAGEN ID
   parses on every row of a real 4.2.4 file** — the resolver in §4 makes the ID load-bearing and currently
   *fails* on an unparseable one, so a single `ID='.'` row would take down a whole partition. Evidence is
   thin (3.10.8 parsed 304/304; one 4.2.4 row seen). If any real file has unparseable IDs, make that path
   **skip-and-count** rather than fail. Three further traps measured in §4:
   **take `SVLEN[0]`**, since LOH rows carry one value per ALT allele and germline's
   `record.INFO.get("SVLEN")` would hand a tuple to a scalar field; **read every ASCN FORMAT field
   defensively**, since `CN`/`MCN` can be missing per-record even when declared; and **skip records whose
   type cannot be resolved**, otherwise `cnv_id` comes back NULL and the StarRocks load fails (§5). The last
   guard is worth applying to germline too, where it is a latent landmine today.
4. **Iceberg table init** — `create_somatic_cnv_occurrence_table()` in
   `radiant/tasks/iceberg/initialization.py`; register in `init_iceberg_tables.py` (**both** the K8s
   and ECS branches — note the ECS branch currently defines its tasks but never chains them),
   `operators/k8s.py`, and the `if/elif` plus both help strings in `scripts/ecs/init_iceberg_table.py`.
5. **Table mappings** — two keys in `radiant/tasks/data/radiant_tables.py`.
6. **StarRocks DDL** — `sql/radiant/init/somatic_cnv_occurrence_create_table.sql` (name must match the
   mapping key).
7. **Load SQL** — the copy-partition and insert-delta pair of §7. Also **update germline's gnomAD-SV join**
   to the same `type`-keyed `CASE` (behaviour-preserving there, and keeps the two files near-copies).
8. **Orchestration** — `import_part.py`: add to `vcf_imports` + the new TaskGroup;
   `operators/{k8s,ecs}.py` + `scripts/ecs/import_somatic_cnv_vcf.py` (+ the Dockerfile copy path).
9. **Data QA** — `radiant/data_qa/sources/somatic_cnv_occurrence.yml`, plus a `type` accepted-values
    test. If we adopt CNLOH/GAINLOH, the dictionary macro needs the new values, and per `CLAUDE.md`
    those lists must stay in sync with the portal's `facets.go` and frontend i18n.
10. **Tests** — `tests/resources/integration/test_somatic_cnv.vcf` covering, at minimum, a GAIN, a LOSS, a
    `.`-ALT REF row, **a legacy-4.2 `<DEL>,<DUP>` LOH row and a 4.4 `<LOH>` row**, asserting both resolve to
    the same `type`/`alternate`/`cnv_id` (§4); unit tests for `process_occurrence` field mapping
    (**germline has none — worth adding
    for somatic rather than copying the omission**); an integration test mirroring
    `test_process_germline_cnv_vcf.py`; conftest schema import + namespace fixture + a
    `clinical_somatic_cnv_vcf` fixture; DAG task-id lists in `tests/unit/dags/test_import_part.py` and
    `test_init_iceberg_tables.py`; clinical seeds for `scnv` documents and a `tumor_only_variant_calling`
    task (**net-new — nothing seeds that task type today**). Seed a task carrying *both* an `scnv` and a
    raw `ssnv` document, so the `WHERE` gate of §3 is actually exercised. Two further tests worth writing
    because their failure mode is remote from its cause: a NULL `cnv_vcf_filepath` must be skipped rather
    than fail the partition (§3), and a two-sample VCF handed to such a task must raise.
11. **Close a coverage gap while here** — `tests/integration/dags/sql/test_queries.py` currently skips
    both CNV SQL templates and omits the CNV table from its init lists, so **no CNV DDL or delta query
    is validated against a live StarRocks today**. Adding somatic doubles the unvalidated surface;
    including both is cheap.

---

## 9. Open questions

**Resolved:** task type is **`tumor_only_variant_calling`** (§3), which also settles how tumor-only is
expressed for CNV; **both VCF versions supported** via the §4 normalisation, with `alternate` a
three-value scalar and `cnv_id` keyed on `type`; **gnomAD-SV joins on `type`**, GAINLOH included (§7); the
**UDF is widened in place** — 3-bit type, 2 bits from `start` (§5).

| # | Question | Why it needs an answer |
|---|---|---|
| 1 | **Does a DRAGEN 4.4 file exist in the cohort?** | The `<LOH>` ALT spelling has never been observed — both characterised files use multi-allelic `<DEL>,<DUP>` — so that input branch is written speculatively and will stay untested until a 4.4 file appears. Answered by asking who runs the sequencing, not by reading a file. |
| 2 | **What is the full DRAGEN version spread?** | At least 3.10.8 and 4.2.4 are both present (§4), and they differ in FORMAT, FILTER vocabulary and LOH support. LOH is therefore **on the critical path**. The spread also tells us how much of the cohort has usable ASCN fields. |
| 3 | **Do the new `CNLOH` / `GAINLOH` type values need portal work first?** | Per `CLAUDE.md`, the QA dictionaries mirror `facets.go` and frontend i18n, so two new `type` values are a cross-repo coordination item, not just an ETL one. |
| 4 | **When do we cut the UDF release + germline backfill?** | Decided *what* (shared UDF, 3-bit type, bits from `start` — §5); still open *when*. Every germline `cnv_id` changes, so the UDF release, both SQL call sites and the backfill have to land together. |
| 5 | **Is the raw somatic SNV VCF from `tumor_only_variant_calling` catalogued as an `ssnv` document?** | Not blocking — the `WHERE` gate of §3 is correct either way — but it decides whether that gate is load-bearing or merely defensive, which is worth knowing before someone "simplifies" it to match the looser germline line. |
| 6 | ~~Volume?~~ **Answered:** 304 segments for a WES sample (§4), so the existing CNV container profile is ample. | — |
| 7 | **Should the somatic CNV load move after somatic SNV in Phase 3/4?** | Required if `nb_snv` joins somatic SNV occurrences (§7). Cheap to get right, annoying to discover late as a silently-zero column. |

---

## 10. Effort and risk

**Effort: low-to-moderate**, and mostly breadth. One table, no frequency layers, no shared-table
race, no clinical-model change, and the two table-registration conventions do real work for us. The
long pole is item count (~12 touchpoints across Python, SQL, DAGs, ECS scripts and tests), not
difficulty.

**Main risks**

- **An unexpected ALT breaks the load, not just one row.** Any ALT other than `<DUP>`/`<DEL>` makes the
  `cnv_id` UDF return NULL against a `NOT NULL` key column (§5). Filter those records in the extractor.
  This is the single most likely way a first run fails.
- **Copying germline verbatim where somatic differs.** Three traps, all quiet. `type` from ALT loses
  CNLOH/GAINLOH with no error, just `UNKNOWN`. `nb_snv` joining the germline SNV table yields
  meaningless numbers. And copying germline's looser `WHERE` — `d.format_code='vcf'` with no data-type
  gate — would let a calling task's raw `ssnv` document populate `vcf_filepath`, quietly giving somatic
  SNV a second ingestion route it must not have (§3). The germline line gets away with that looseness
  only because of what upstream happens to register, not by design.
- **We are designing against QLIN's model, not a file — and one of their claims may not hold.** Their
  raw schema was code-generated in 2022 and their tests never load a real CNV VCF; they build DataFrames
  from case-class defaults. So QLIN tells us reliably what they *keep*, and unreliably what DRAGEN
  *emits*. The CNLOH case in §4 is the live example: an ALT filter running ahead of the type derivation
  may mean their documented LOH support never actually fires. **Treat QLIN as a design reference, not as
  evidence about the data.** Item 1 of §8 closes this.
- **Inherited CNV debt doubles.** Accepted per §6, but it should be an explicit, tracked consequence
  rather than a discovery later.

---

## 11. How we'd verify

- `make test-unit` — task-model validation (tumor-only accepted, tumor-normal and normal-only
  rejected), `process_occurrence` field mapping incl. a CNLOH row, DAG task-id assertions.
- `USE_DOCKER_FIXTURES=true make test-integration` — extraction against the local Iceberg REST
  catalog + MinIO: table created, rows written, `type` domain correct, tenant/seq/task tagging right.
- The same run also covers the SQL: with the load-SQL and test-coverage items done, `test_queries.py` validates the new DDL
  and `EXPLAIN`s the delta query against a live StarRocks.
- `make test-docker` — the full compose stack end to end, driven by seeded `scnv` documents.
- Manual spot-check on a real file once available: segment count in, row count out, and
  `type` / `filter` value distributions against the DRAGEN dictionaries in §4.
