# radiant_vcf — germline and somatic SNV extraction in Rust (POC)

A Python extension (PyO3 + maturin) that replaces the SNV extractions of the import:

| flavour | Python path | tables |
|---|---|---|
| germline | `radiant/tasks/vcf/snv/germline/process.py` | `germline_snv_occurrence`, `snv_variant`, `snv_consequence`, `germline_snv_comphet` |
| somatic | `radiant/tasks/vcf/snv/somatic/process.py` | `somatic_snv_occurrence`, `snv_variant`, `snv_consequence` |

It reads the VCF with htslib, builds the rows as Arrow batches, and writes them to Parquet with the arrow-rs
`parquet` crate.

```
VCF ──htslib──▶ Arrow builders ──cast to Iceberg schema──▶ parquet::ArrowWriter ──▶ local files      (Rust, no GIL)
                                                                                       │
                     PartitionCommit (paths) ◀── upload (PyArrowFileIO) ◀──────────────┘          (Python)
                     commit_partitions → tx.delete + tx.add_files                                  (unchanged)
```

The Iceberg commit does not change: `add_files` reads each file's footer (schema, stats, partition values), so the
files only need the table's Arrow schema without field ids and column statistics, as the Python writer gives.
`sink_config` (`radiant/tasks/vcf/snv/rust_engine.py`) takes the writer options from the table properties as
`_write_file` does (`_get_parquet_writer_kwargs`: codec, level, page and dictionary sizes, page row limit; row-group
limit; target file size). `upload_files` names and places the files as `_write_file` does (location provider,
partition path, `00000-<n>-<uuid>.parquet`). iceberg-rust is not used.

Two flush rules differ, on purpose: a row group is also closed at `RADIANT_PARQUET_FILE_SIZE_MB` of encoded data
(the Python path's memory bound becomes the writer's), and a file is closed at `write.target-file-size-bytes` of
Parquet bytes (the Python path bins on in-memory Arrow bytes, so it writes more, smaller files).

## Layout

| file | ports |
|---|---|
| `src/snv.rs` | what both flavours share: record checks, `process_common`, `InfoKeys`, genotype/AD views, `process_variant`, the `snv_variant` / `snv_consequence` builders |
| `src/germline.rs` | germline `process_occurrence` (pedigree, parental origin, transmission mode), `process_comphet` |
| `src/somatic.rs` | somatic `process_occurrence` (tumor / normal columns, `read_hotspot`, `adjust_somatic_calls_and_zygosity`) |
| `src/consequence.rs` | `process_consequence`, `parse_csq_header`, `resolve_source`, `parse_cds_position` |
| `src/genotype.rs`, `src/hts.rs` | the cyvcf2 v0.31.1 properties the Python code reads, on htslib's raw buffers |
| `src/inheritance.rs`, `src/tables.rs` | the ETL and slivar inheritance rules; `tables.rs` is generated from `occurrence.py` by `gen_tables.py` |
| `src/parquet_sink.rs` | the Parquet write half of `_write_file` |

The somatic sample checks (missing aliquot, tumor-only task on a multi-sample VCF, histology types) stay in Python:
`somatic/process.py::check_samples` is called by both engines, so the errors are the same.

## Use

```sh
pip install maturin
export PATH="$HOME/.cargo/bin:$PATH"
make rust-develop                    # maturin develop --release into the active venv
make rust-test                       # Rust unit tests + the Python engine and parity tests
make rust-bench                      # germline and somatic benchmarks
RADIANT_SNV_ENGINE=rust <run the extraction>
```

`RADIANT_SNV_ENGINE` is read by both `process_task` on each call: `python` (default) or `rust`. Any other value
raises. With `rust` and no extension installed, the import fails: there is no silent fallback.

Image: `Dockerfile.radiant.operator` builds the wheel in a cross-compiling stage (native build host, target
`x86_64-unknown-linux-gnu`) and installs it. `--build-arg RADIANT_SNV_ENGINE=rust` sets the image default; without it
the image keeps `python`. The SNV operators (K8s and ECS) pass `RADIANT_SNV_ENGINE` from the Airflow environment when
it is set, so an environment switches engines without a new image; unset, the image value applies.

## Parity

The Rust code is a port of the Python code and of the cyvcf2 v0.31.1 properties it reads, quirks included
(`strict_gt` genotype types, the C `int` sum of `AD`, INFO scalar rules, raw `FORMAT/SQ` values). One quirk is
fixed in both engines instead: see `phased` below. Each function names the Python or cyvcf2 code it ports.

- `tests/unit/vcf/germline/test_rust_engine_parity.py` and `tests/unit/vcf/somatic/test_rust_engine_parity.py` run
  the real `process_task` through both engines. Python rows: the Arrow table each `TableAccumulator` would write.
  Rust rows: its Parquet files, read back. They must be equal row by row (NaN equals NaN), with the Iceberg Arrow
  schema. Germline inputs: every germline test VCF under several pedigrees, files both engines must reject, and
  6 × 3000 random trio records. Somatic inputs: the tumor-normal VCF (normal in the first column, experiments in
  both orders), the tumor-only VCF, the five sample-check errors and a missing CSQ line, and 2 × 3000 random records
  (tumor-normal and tumor-only: every zygosity, missing SQ, `hotspot` flag and `HotspotAllele`, somatic INFO fields).
  Both skip themselves when the extension is not built.
- `tests/unit/vcf/test_rust_engine.py`: engine switch, writer options, partition key.
- `tests/integration/test_process_{germline,somatic}_snv_vcf.py` with `RADIANT_SNV_ENGINE=rust` (Docker fixtures:
  real upload, `add_files` commit, read back): 10 of 11 pass. `test_process_task_error` (germline) fails by
  construction: it injects the htslib error through `TableAccumulator.write_files`, which the Rust path does not call.

### Sandbox (radiant-portal-sandbox, minikube)

`radiant-import-snv-vcf` run on the sandbox tasks, once with an image whose default engine is `python`, once with
`rust`, same code. The Iceberg content of each table and task (row count and two order-independent hashes over every
column, through StarRocks) is identical between the two runs.

| task | flavour | rows (occurrence / variant / consequence) | writer pod, python | writer pod, rust |
|---|---|---|---|---|
| 63 | germline trio | 977 451 / 325 817 / 1 497 880 | 91.7 s | 10.5 s |
| 65 | germline solo | 243 720 / 243 720 / 1 111 666 | 55.8 s | 8.1 s |
| 67 | somatic tumor-normal | 178 023 / 178 023 / 731 516 | 36.8 s | 8.1 s |
| 68 | somatic tumor-only | 134 599 / 134 599 / 605 219 | 29.4 s | 7.1 s |
| 69 | somatic tumor-only | 131 024 / 131 024 / 568 791 | 27.1 s | 7.1 s |

Pod time includes the pod start and the S3 download of the VCF.

These runs predate the CA1000019 seed: task 63 (documents 135/136) now reads
`variants.CA1000019.snv.vep.slivar.aliquots.vcf.gz`, which the sandbox repo must ship before
`init-qa-clinical-data` runs again.

## Benchmark

`make rust-bench`. Both engines write Parquet to a local directory (Python: pyarrow with the options of
`_write_file`); the S3 upload is left out. `vcf_threads=4`, macOS arm64, one run each:

| flavour | input | engine | time | peak RSS | Parquet |
|---|---|---|---|---|---|
| germline | 100 000 records of a VEP `--merged` trio (`test_consequence_merged.vcf`), 3.39 M consequence rows | python | 24.1 s | 1 081 MB | 15.3 MB |
| | | rust | 4.5 s | 980 MB | 15.1 MB |
| somatic | 200 000 Mutect2 tumor-normal records (`test_somatic_snv.vcf`), 2.02 M consequence rows | python | 17.3 s | 940 MB | 19.2 MB |
| | | rust | 3.0 s | 679 MB | 19.1 MB |

5.3× (germline) and 5.7× (somatic) faster. The htslib decode alone (cyvcf2, iterate only) takes about 1.0 s of the
germline run.

## Known differences

- Two experiments of a germline task with the same `seq_id`: the Python path merges them silently, the Rust one raises.
- `FORMAT/DP` or `FORMAT/GQ` declared `Float`, `FORMAT/SQ` declared `Integer`, or `INFO/HotspotAllele` declared
  `Float`: the Rust engine raises, the Python engine loads the file (a Float `DP`/`GQ` is cut to a whole number). Use
  the Python engine for such a file.
- Logging: the per-record `debug` lines for skipped records are replaced by end-of-file counters, and malformed
  `slivar_comphet` entries are reported in one warning with 5 examples, not one warning each.
- File layout: fewer, larger files (see the flush rules above). The rows are the same.

## Found while porting (pre-existing, not changed)

- `snv_variant.rsnumber` is `list<string>` but `process_variant` stores the VCF `ID` string in it, and pyarrow reads
  a Python `str` as a sequence: `rs123` is stored as `["r", "s", "1", "2", "3"]`. The Rust engine reproduces it for
  parity. Downstream, `snv_staging_variant_insert.sql` takes `rsnumber` from dbSNP, so the column of the Iceberg
  table looks unused, but it is wrong data.
- `SEXUAL_TRANSMISSION_LOOKUP` (germline `occurrence.py`) is keyed on `Male` / `Female`, but the experiments carry the
  clinical `sex_code` (`male` / `female`, see `seeds.sql`; `nextflow/qc/inputs.py` maps them for that reason). The
  ETL transmission mode on X and Y never matches and is null. Both engines do the same.
- `somatic_snv_occurrence.tumor_sq` / `normal_sq`: a missing `FORMAT/SQ` value (`.`) is stored as NaN, not null,
  because cyvcf2's `format()` returns htslib's missing-float bit pattern, a NaN. The Rust engine reproduces it.
- Somatic `occurrence.py` reads `INFO/CAL` (`info_cal`), but the table has no such column, so the value is dropped.

## Found while porting (fixed in both engines)

- `phased` / `tumor_phased` / `normal_phased` of a haploid genotype: cyvcf2's `gt_phases` reads the phase bit of the
  second allele whatever the ploidy, so a haploid sample took the next sample's bit, and the last sample read past
  the genotype array (undefined: the parity tests failed on Linux, not on macOS). `vcf_utils.sample_phased` and
  `genotype.rs::phased` now give False for a haploid sample. Rows written before this change keep their value until
  the task is imported again.

## Not done (POC scope)

- CI: the `rust-tests` job of `test.yml` builds the extension and runs `make rust-test` and the SNV integration
  tests with `RADIANT_SNV_ENGINE=rust`; the `run-tests` job skips the parity tests (no extension). Trivy and the
  release gate do not scan the Rust dependencies yet (maturin has no `cargo auditable` build).
