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

Also out of scope, and tracked separately: **two pre-existing germline CNV bugs that this work inherits by
copying germline's shape.** One fix in each repairs germline and somatic together, which is why they are
their own tickets rather than part of this one.

- **SJRA-1784** *(high)* — a failed VCF download is skipped, but the Airflow task still succeeds, so the
  experiment is marked ingested and per SJRA-1187 the incremental delta never offers it again. Silent,
  permanent data loss.
- **SJRA-1785** *(low)* — deleted tasks are still downloaded and extracted. StarRocks output stays correct
  (the partition swap excludes deleted `seq_id`s), so the cost is wasted work, not bad data.

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
  sd, ascn_as`), all nullable and often empty. `filter` stays `VARCHAR(255)` like every other occurrence
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

### Validating the file against the task type

The task type asserts tumor-only, but the file should be checked against it. Three cheap checks:

1. exactly one experiment on the task;
2. that experiment's `histology_type == 'tumoral'` — stops a mislabelled normal-only task writing normal
   depths as tumor values;
3. the VCF declares exactly one sample — capture `vcf.samples` **before** `set_samples` narrows it (the
   trick in `snv/somatic/process.py:44-49`), since narrowing rewrites `vcf.samples` and `vcf.raw_header`
   and destroys the evidence.

Check 3 is the only one that catches a genuinely tumor-normal file mislabelled upstream. Tumor-normal CNV
is out of scope, so fail loudly rather than process the tumor sample and half-succeed.

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

But **`CN` and `MCN` cannot be relied on.** DRAGEN 3.10.8 does not emit them at all (FORMAT is
  `GT:SM:BC:PE`); 4.2.4 declares them but omits them from the LOH row's own FORMAT. 

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


**The residual risk of depending on the ID.** The VCF spec treats it as **optional** (`.` when absent), and
for symbolic alleles it is the **ALT** that carries the event type — the ID carries an identifier, strictly
needed only to link breakend mates via `MATEID`. `DRAGEN:<TYPE>:<chr>:<start>-<end>` is a vendor convention,
not a standard; no documentation shows GATK gCNV, CNVkit or Canvas populating ID with a parseable type. We
depend on it anyway because, as measured above, nothing else in the file can classify LOH. Worth recording
as a known coupling to DRAGEN rather than pretending we have a fallback.

**The way out, if another caller ever has to be supported, is `CN` + `MCN` rather than the ID.** `MCN=0`
marks the loss of heterozygosity, and `CN` against expected ploidy separates CNLOH from GAINLOH. That is the
vendor-neutral formulation — it simply does not work on the DRAGEN files we have, which either omit those
fields entirely (3.10.8) or populate them per-record (4.2.4). **Out of scope for now**, and noted here only
so the escape hatch is on record rather than rediscovered later.

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


### Column set

Germline CNV's columns, plus the DRAGEN 4.2.4 ASCN fields. The base is unchanged from germline:

> `part`, `seq_id`, `tenant_code`, `task_id`, `aliquot`, `chromosome`, `alternate`, `start`, `end`,
> `type`, `length`, `name`, `quality`, `calls`, `bc`, `pe`, `sm`, `svtype`, `svlen`, `reflen`, `phased`

Where somatic differs, and what the two measured files say:

| Column | Decision | Observed |
|---|---|---|
| `type` | `GAIN` / `LOSS` / `CNLOH` / `GAINLOH`, from the DRAGEN ID (§4) | all four occur in production; the ID is the only reliable source |
| `alternate` | `<DUP>` / `<DEL>` / `<LOH>` — both LOH spellings normalised (§4) | 4.2 writes `<DEL>,<DUP>`, 4.4 writes `<LOH>` |
| `cnv_id` | keyed on `type`, not `alternate` | requires the widened UDF below |
| `cn, cnf, cnq, mcn, mcnf, mcnq, maf, sd, ascn_as` | added, all nullable | ASCN fields: absent in 3.10.8, per-record in 4.2.4. `MAF=0` is the direct LOH marker. DRAGEN's FORMAT `AS` is stored as **`ascn_as`**: `as` is reserved in both Python and StarRocks/MySQL and would need quoting in every DDL, load-SQL and dbt call site |
| `cipos` / `ciend` | kept, nullable | 3.10.8 declares but never populates them; 4.2.4 does not declare them at all |
| `filter` | `VARCHAR(255)`, as in every other occurrence table | 22% of file A's rows are multi-valued, semicolon-joined |

Three things that follow:

- **QA except-list.** `cn`, `mcn`, `cipos`, `ciend` and the ASCN floats are entirely NULL on older DRAGEN, so
  they need exempting from `should_not_contain_only_null` — germline already does this for
  `svlen`/`cipos`/`ciend` as "constant by construction".
- **`cipos`/`ciend` are candidates for dropping outright**, since no observed version populates them.
- **`filter` needs a split-and-check QA test**, not `accepted_values`: the value is semicolon-joined, so an
  enumerated test would have to list *combinations* rather than values, and the vocabulary is
  version-dependent (§4). Germline sidesteps this by not testing `filter` at all.

Open (§9 Q1): whether an LOH-bearing file carries `CN`/`MCN`. The fully measured file has no LOH.

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

---

## 7. Load and annotations — *decided: structural + keep gnomAD-SV*

Two new SQL templates, near-copies of the germline pair:

- `somatic_cnv_occurrence_copy_partition.sql` — the 3-line survivor copy, verbatim
- `somatic_cnv_occurrence_insert_partition_delta.sql` — the enrichment query

Wired into `import_part.py` as a new TaskGroup mirroring `germline_cnv_occurrence`: a
`sanity_check_somatic_cnvs` short-circuit on the new `task_type`, then a
`RadiantStarRocksPartitionSwapOperator.partial(...).expand_kwargs(tenant_params)`. Placement is §9 Q7:
next to the germline CNV group, or later if `nb_snv` needs somatic SNV occurrences to exist first.

The enrichment carries over, with **one substantive change**:

| Annotation | Somatic |
|---|---|
| `cytoband` | same — join `starrocks_cytoband` on overlap |
| `symbol` / `nb_genes` | same — join `starrocks_ensembl_gene` on overlap |
| `nb_snv` | **must join `iceberg_somatic_snv_occurrence`, not the germline one** |
| `gnomad_af/sc/sn/sf/sc_hom/sc_het` | **kept**, same 80% reciprocal-overlap logic, but the join moves off `alternate` — see below |

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

One item per sub-task of **[SJRA-1770](https://d3b.atlassian.net/browse/SJRA-1770)**, roughly in dependency order. Nothing is large; the
count is the cost, not any single item. The reasoning for each lives in the referenced section.

1. **[SJRA-1772](https://d3b.atlassian.net/browse/SJRA-1772) — Discovery and task model.** The two lines of §3 in
   `staging_external_sequencing_experiment_create_table.sql`, plus the new constant, `BaseTask` subclass and
   `_TASK_TYPES` entry in `vcf/experiment.py`, with §3's validations and a nullable `cnv_vcf_filepath`.
2. **[SJRA-1773](https://d3b.atlassian.net/browse/SJRA-1773) — Extraction.** `radiant/tasks/vcf/cnv/somatic/{occurrence,process}.py`,
   avoiding the traps of §4 and using a field-ID range clear of germline CNV's 100-124 / 200-203.
3. **[SJRA-1774](https://d3b.atlassian.net/browse/SJRA-1774) — Tables.** `create_somatic_cnv_occurrence_table()` registered in
   `init_iceberg_tables.py` (**both** branches — the ECS one defines its tasks but never chains them), two
   keys in `radiant_tables.py`, and `somatic_cnv_occurrence_create_table.sql` named to match the mapping key.
4. **[SJRA-1775](https://d3b.atlassian.net/browse/SJRA-1775) — Load SQL.** The copy-partition and insert-delta pair of §7, plus the same
   `type`-keyed gnomAD join applied to germline.
5. **[SJRA-1776](https://d3b.atlassian.net/browse/SJRA-1776) — Orchestration.** `import_part.py`, `operators/{k8s,ecs}.py`,
   `scripts/ecs/import_somatic_cnv_vcf.py` and the Dockerfile copy path.
6. **[SJRA-1777](https://d3b.atlassian.net/browse/SJRA-1777) — `cnv_id` UDF.** Widen to a 3-bit type field (§5), update both SQL call
   sites, and backfill germline `cnv_id`s — these three land together.
7. **[SJRA-1778](https://d3b.atlassian.net/browse/SJRA-1778) — Tests, data QA and seeds.** A fixture VCF covering GAIN, LOSS, a `.`-ALT
   row and **both** LOH spellings; `somatic_cnv_occurrence.yml` per §5; seeds are net-new and should carry
   both an `scnv` and a raw `ssnv` document so the §3 gate is exercised; and close the `test_queries.py` gap,
   where no CNV DDL or delta query is validated against a live StarRocks today.

## 9. Open questions

**Resolved:** task type is **`tumor_only_variant_calling`** (§3), which also settles how tumor-only is
expressed for CNV; **both VCF versions supported** via the §4 normalisation, with `alternate` a
three-value scalar and `cnv_id` keyed on `type`; **gnomAD-SV joins on `type`**, GAINLOH included (§7); the
**UDF is widened in place** — 3-bit type, 2 bits from `start` (§5).

| # | Question | Why it needs an answer |
|---|---|---|
| 1 | **Do the new `CNLOH` / `GAINLOH` type values need portal work first?** | Per `CLAUDE.md`, the QA dictionaries mirror `facets.go` and frontend i18n, so two new `type` values are a cross-repo coordination item, not just an ETL one. |


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
