# radiant_vcf — germline SNV extraction in Rust (POC)

A Python extension (PyO3 + maturin) that replaces the germline SNV extraction
(`radiant/tasks/vcf/snv/germline/process.py` and the modules it calls). It reads the VCF with htslib, builds the
rows of the four tables (`germline_snv_occurrence`, `snv_variant`, `snv_consequence`, `germline_snv_comphet`) as
Arrow batches, and writes them to Parquet with the arrow-rs `parquet` crate.

```
VCF ──htslib──▶ Arrow builders ──cast to Iceberg schema──▶ parquet::ArrowWriter ──▶ local files      (Rust, no GIL)
                                                                                       │
                     PartitionCommit (paths) ◀── upload (PyArrowFileIO) ◀──────────────┘          (Python)
                     commit_partitions → tx.delete + tx.add_files                                  (unchanged)
```

The Iceberg commit does not change: `add_files` reads each file's footer (schema, stats, partition values), so the
files only need the table's Arrow schema without field ids and column statistics, as the Python writer gives.
`sink_config` (`rust_engine.py`) takes the writer options from the table properties as `_write_file` does
(`_get_parquet_writer_kwargs`: codec, level, page and dictionary sizes, page row limit; row-group limit; target file
size). `upload_files` names and places the files as `_write_file` does (location provider, partition path,
`00000-<n>-<uuid>.parquet`). iceberg-rust is not used.

Two flush rules differ, on purpose: a row group is also closed at `RADIANT_PARQUET_FILE_SIZE_MB` of encoded data
(the Python path's memory bound becomes the writer's), and a file is closed at `write.target-file-size-bytes` of
Parquet bytes (the Python path bins on in-memory Arrow bytes, so it writes more, smaller files).

## Use

```sh
pip install maturin
make rust-develop                    # maturin develop --release into the active venv
RADIANT_SNV_ENGINE=rust <run the extraction>
```

`RADIANT_SNV_ENGINE` is read by `process_task` on each call: `python` (default) or `rust`. Any other value raises.
With `rust` and no extension installed, the import fails: there is no silent fallback.

Image: `Dockerfile.radiant.operator` builds the wheel in a cross-compiling stage (native build host, target
`x86_64-unknown-linux-gnu`) and installs it. `--build-arg RADIANT_SNV_ENGINE=rust` sets the image default; without it
the image keeps `python`. The DAG operators do not pass the variable, so the image value applies.

## Parity

The Rust code is a port of the Python code and of the cyvcf2 v0.31.1 properties it reads, quirks included
(`strict_gt` genotype types, the C `int` sum of `AD`, the `gt_phases` read of the next value, INFO scalar rules).
Each function names the Python or cyvcf2 code it ports. The inheritance lookup tables are generated from
`occurrence.py` by `gen_tables.py`; a unit test fails when `src/tables.rs` is stale.

- `tests/unit/vcf/germline/test_rust_engine_parity.py` runs the real `process_task` through both engines. Python
  rows: the Arrow table each `TableAccumulator` would write. Rust rows: its Parquet files, read back. They must be
  equal row by row, with the Iceberg Arrow schema. Inputs: every germline test VCF under several pedigrees (solo,
  duo, trio, sample subset, proband in another column), files both engines must reject, and 6 × 3000 random trio
  records (genotypes incl. haploid, missing and phased; `AD` shapes; INFO types and aliases; FILTER; QUAL; slivar
  tags; `slivar_comphet` with malformed entries). Skips itself when the extension is not built.
- `tests/unit/vcf/germline/test_rust_engine.py`: engine switch, writer options, partition key.
- `tests/integration/test_process_germline_snv_vcf.py` with `RADIANT_SNV_ENGINE=rust` (Docker fixtures: real upload,
  `add_files` commit, read back): 5 of 6 pass. `test_process_task_fails_on_htslib_error` fails by construction: it
  injects the htslib error through `TableAccumulator.write_files`, which the Rust path does not call.

## Benchmark

`make rust-bench`: 100 000 records of a VEP `--merged` trio (the records of `test_consequence_merged.vcf` copied),
3.39 M consequence rows, `vcf_threads=4`. Both engines write Parquet to a local directory (Python: pyarrow with the
options of `_write_file`); the S3 upload is left out. macOS arm64, one run each:

| engine | time | records/s | peak RSS | Parquet |
|---|---|---|---|---|
| python | 25.7 s | 3 895 | 1 039 MB | 15.3 MB |
| rust | 4.8 s | 21 024 | 1 066 MB | 15.1 MB |

5.4× faster. Extraction alone (Parquet left out) was 23.6 s vs 3.55 s; the htslib decode alone (cyvcf2, iterate
only) takes about 1.0 s. Peak RSS was not profiled.

## Known differences

- Two experiments of a task with the same `seq_id`: the Python path merges them silently, the Rust one raises.
- `FORMAT/DP` or `FORMAT/GQ` declared `Float`: the Rust engine raises. The Python behaviour for this case was not
  checked (it hands a numpy float to an int32 column).
- Logging: the per-record `debug` lines for skipped records are replaced by end-of-file counters, and malformed
  `slivar_comphet` entries are reported in one warning with 5 examples, not one warning each.
- File layout: fewer, larger files (see the flush rules above). The rows are the same.

## Found while porting (pre-existing, not changed)

- `snv_variant.rsnumber` is `list<string>` but `process_variant` stores the VCF `ID` string in it, and pyarrow reads
  a Python `str` as a sequence: `rs123` is stored as `["r", "s", "1", "2", "3"]`. The Rust engine reproduces it for
  parity. Downstream, `snv_staging_variant_insert.sql` takes `rsnumber` from dbSNP, so the column of the Iceberg
  table looks unused, but it is wrong data.
- `SEXUAL_TRANSMISSION_LOOKUP` (`occurrence.py`) is keyed on `Male` / `Female`, but the experiments carry the
  clinical `sex_code` (`male` / `female`, see `seeds.sql`; `nextflow/qc/inputs.py` maps them for that reason). The
  ETL transmission mode on X and Y never matches and is null. Both engines do the same.

## Not done (POC scope)

- CI: the parity test skips without the extension; a CI job would need cargo + `maturin develop`. Trivy and the
  release gate do not scan the Rust dependencies yet.
- Somatic SNV (`snv/somatic/`) shares `consequence`, `variant` and `common`; it is the next candidate.
