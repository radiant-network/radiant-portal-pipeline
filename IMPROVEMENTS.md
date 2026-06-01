# Low-Hanging Fruit Improvements

## Performance

### P1. Linear search in Pedigree — O(n*m) → O(n+m)
**File:** `radiant/tasks/vcf/pedigree.py:30-33`

```python
# Current: O(n*m)
for vcf_sample in vcf_samples:
    experiment = next((exp for exp in task.experiments if exp.aliquot == vcf_sample), None)

# Fix: build lookup dict
exp_lookup = {exp.aliquot: exp for exp in task.experiments}
```

### P2. Repeated INFO field access in hot loop
**Files:** `radiant/tasks/vcf/snv/germline/occurrence.py:126-144`, `somatic/occurrence.py:159-183`

23 separate `record.INFO.get()` calls per VCF record. On multi-million-record files this adds up. Batch-extract into a dict once per record.

### P3. Redundant Arrow table reconstruction in TableAccumulator
**File:** `radiant/tasks/iceberg/table_accumulator.py:130`

`pa.Table.from_pylist(self.rows, schema=self.schema)` rebuilds Arrow table from Python lists every 10K rows. Consider `pa.RecordBatchBuilder` for incremental construction — reduces memory allocations on large datasets.

### P4. Catalog loaded fresh per VCF task
**Files:** `radiant/tasks/vcf/snv/germline/process.py:41-42`, `somatic/process.py`, `cnv/germline/process.py:35-37`

`load_catalog()` called each time. Cache per `(catalog_name, properties)` tuple or pass as parameter to avoid reconnection overhead when processing multiple partitions.

### P5. Three separate Iceberg commits per partition
**Files:** `radiant/tasks/vcf/snv/germline/process.py:65-74`, `somatic/process.py:62-71`

Occurrence, variant, and consequence tables each get separate write+commit. Batching into a single Iceberg transaction would reduce commit overhead and improve atomicity.

---

## Code Cleanup

### C1. Massive duplication between germline and somatic occurrence processing
**Files:** `radiant/tasks/vcf/snv/germline/occurrence.py` (665 lines) vs `somatic/occurrence.py` (329 lines)

~70% code overlap:
- INFO field extraction (19 identical `.get()` calls)
- FORMAT field extraction (`record.format("DP")[idx][0] if "DP" in record.FORMAT else 0` repeated everywhere)
- `normalize_calls()` function — identical in both files
- Schema definitions share common NestedField blocks

**Fix:** Extract shared logic into a base module:
- `extract_info_fields(record) -> dict`
- `get_format_value(record, field, idx, default=0)`
- Common schema field definitions
- Base `process_occurrence()` with hooks for germline/somatic-specific logic

### C2. Duplicated `process_task()` pipelines
**Files:** `germline/process.py` (99 lines) vs `somatic/process.py` (103 lines)

95% identical structure: load catalog → open VCF → create buffers → iterate records → write → commit. Differs only in table names, pedigree vs experiment sorting, and occurrence processor function.

**Fix:** Parameterized `process_vcf_task()` accepting task type config, occurrence processor, and table names.

### C3. Lookup tables spanning 215 lines with no documentation
**File:** `radiant/tasks/vcf/snv/germline/occurrence.py:451-665`

`AUTOSOMAL_ORIGINS_LOOKUP`, `X_ORIGINS_LOOKUP`, `Y_ORIGINS_LOOKUP`, `AUTOSOMAL_TRANSMISSION_LOOKUP`, `SEXUAL_TRANSMISSION_LOOKUP` — magic tuple keys with no explanation of what each entry means.

**Fix:** Either:
- Convert to explicit logic functions with documented conditions
- Or move to a separate data module with per-entry comments

### C4. Confusing operator precedence in zygosity adjustment
**File:** `radiant/tasks/vcf/snv/germline/occurrence.py:245-283`

```python
# Current — precedence is ambiguous
if (ad_alt and (zygosity in (...) and ad_alt < 3) or ad_ref and zygosity == ZYGOSITY_WT and ad_ref < 3):

# Fix — explicit grouping
insufficient_alt = ad_alt and zygosity in (ZYGOSITY_HET, ZYGOSITY_HOM) and ad_alt < 3
insufficient_ref = ad_ref and zygosity == ZYGOSITY_WT and ad_ref < 3
if insufficient_alt or insufficient_ref:
```

### C5. Unused imports in somatic process
**File:** `radiant/tasks/vcf/snv/somatic/process.py:3-4`

- `defaultdict` imported but never used
- `commit_files` imported but never called

### C6. Missing tracing in somatic pipeline
**File:** `radiant/tasks/vcf/snv/somatic/process.py`

Germline process uses `get_tracer` for OpenTelemetry spans. Somatic process has no tracing — inconsistent instrumentation.

### C7. Inconsistent error handling in S3 utilities
**File:** `radiant/tasks/utils.py:55-104`

- `download_s3_file()` returns `None` on error
- `download_json_from_s3()` raises exception
- `delete_s3_object()` silently logs warning

**Fix:** Standardize on exception-based error handling with a custom `S3OperationError`.

### C8. Potential null-safety issue in consequence parsing
**File:** `radiant/tasks/vcf/snv/consequence.py:104`

`get_csq_field(..., "EXON").split("/")` — no null check before `.split()`. `get_csq_field` can return `None`, causing `AttributeError` at runtime.

---

## Testability

### T1. `process_task()` is untestable without external services
**Files:** `germline/process.py:30-128`, `somatic/process.py:32-134`

Single function handles: VCF I/O, catalog loading, buffer creation, record processing, Parquet writing, Iceberg commit. Cannot test transformation logic without spinning up Iceberg + S3.

**Fix:** Separate pure data transformation from I/O:
```python
# Testable without external services
def process_records(vcf_iter, experiments) -> ProcessedData: ...

# I/O boundary
def persist(processed_data, catalog, tables) -> CommitResult: ...
```

### T2. No unit tests for critical components
**Missing test coverage:**
- `radiant/tasks/iceberg/table_accumulator.py` — 182 lines, threshold-based merge/write logic, schema manipulation
- `radiant/tasks/iceberg/initialization.py` — 160 lines, partition spec creation with hardcoded field IDs
- `radiant/tasks/vcf/pedigree.py` — only tested implicitly through occurrence tests
- `radiant/tasks/data/radiant_tables.py` — configuration mapping functions

### T3. Hardcoded magic numbers block threshold testing
| Constant | Location | Value |
|----------|----------|-------|
| `ad_alt/ad_ref` threshold | `germline/occurrence.py:~270` | `3` |
| `MAX_BUFFERED_ROWS` | `table_accumulator.py` | `10000` |
| `PARQUET_FILE_SIZE_MB` | `table_accumulator.py` | `500` |

**Fix:** Extract to named constants or constructor parameters so tests can exercise boundary conditions.

### T4. `os.environ` reads scattered through initialization code
**File:** `radiant/tasks/iceberg/initialization.py`

Six functions each call `os.environ["RADIANT_ICEBERG_NAMESPACE"]` directly. No fallback, no injection point.

**Fix:** Accept namespace as parameter with env-var default:
```python
def create_variant_table(catalog, namespace=None):
    namespace = namespace or os.environ["RADIANT_ICEBERG_NAMESPACE"]
```

### T5. Lookup tables untestable for edge cases
**File:** `radiant/tasks/vcf/snv/germline/occurrence.py:451-665`

`parental_origin()` and `compute_transmission_mode()` use module-level dicts. Testing missing-key behavior or alternative lookup rules requires monkeypatching module state.

**Fix:** Accept lookup table as optional parameter:
```python
def parental_origin(chrom, p_calls, f_calls, m_calls, lookup=None):
    lookup = lookup or _default_lookup_for(chrom)
```

### T6. Integration test uses ctypes to capture C stderr — brittle
**File:** `tests/integration/` (error logging tests)

`fake_error_logging()` manipulates C-level stderr via `ctypes`. Platform-dependent and fragile.

**Fix:** Mock at Python logging layer instead.

### T7. ~55% of functions in `radiant/tasks/` lack direct test coverage
Per analysis: ~111 functions, ~50 tested. Key gaps are in Iceberg utilities, table initialization, and the transmission/parental-origin logic edge cases.

### T8. Fixture duplication across integration conftest files
**File:** `tests/integration/conftest.py` (463 lines)

`clinical_snv_vcf()`, `clinical_cnv_vcf()`, `clinical_exomiser_tsv()` follow identical query-then-upload patterns. `mapping_conf` fixture duplicates production config construction.

**Fix:** Extract shared fixture helpers. Consider a builder pattern for test data setup.

---

## Priority Matrix

| ID | Category | Effort | Impact | Priority |
|----|----------|--------|--------|----------|
| C1 | Cleanup | Medium | High | **1** |
| C2 | Cleanup | Low | High | **2** |
| T1 | Testability | Medium | High | **3** |
| P1 | Performance | Trivial | Medium | **4** |
| T2 | Testability | Medium | High | **5** |
| C4 | Cleanup | Trivial | Medium | **6** |
| C5 | Cleanup | Trivial | Low | **7** |
| T3 | Testability | Low | Medium | **8** |
| T4 | Testability | Low | Medium | **9** |
| P2 | Performance | Low | Medium | **10** |
| P4 | Performance | Low | Low | **11** |
| C3 | Cleanup | Medium | Medium | **12** |
| C6 | Cleanup | Low | Low | **13** |
| C7 | Cleanup | Low | Low | **14** |
| C8 | Cleanup | Trivial | Medium | **15** |
| P3 | Performance | Medium | Medium | **16** |
| P5 | Performance | Medium | Medium | **17** |
| T5 | Testability | Low | Medium | **18** |
| T6 | Testability | Low | Low | **19** |
| T7 | Testability | High | High | **20** |
| T8 | Testability | Low | Low | **21** |
