"""Time the SNV extraction of both engines on a synthetic VCF built from real records.

    python rust/radiant_vcf/bench.py [--flavour germline|somatic] [--records 100000] [--engine python|rust|both]

germline: the records of tests/resources/test_consequence_merged.vcf (a trio, VEP --merged CSQ). somatic: the records
of tests/resources/test_somatic_snv.vcf (Mutect2 tumor-normal). They are copied with new positions until the file
holds `--records` records, then gzipped. Each engine runs `process_task` in its own process with a fake catalog. Both
write the Parquet files to a local directory (Python: pyarrow with the options of `_write_file`; Rust: the parquet
crate); the S3 upload is left out. Reports wall time, records per second, peak RSS and Parquet size.
"""

import argparse
import gzip
import itertools
import json
import os
import resource
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = {
    "germline": ROOT / "tests" / "resources" / "test_consequence_merged.vcf",
    "somatic": ROOT / "tests" / "resources" / "test_somatic_snv.vcf",
}


def build_vcf(template: Path, path: Path, n_records: int):
    header, body = [], []
    for line in template.read_text().splitlines():
        (header if line.startswith("#") else body).append(line.split("\t"))
    # The template spans several chromosomes: each copy gets its own positions, 100 bp apart, along each autosome
    # in turn, so they stay sorted and far below the contig length.
    span = 100 * (len(body) + 1)
    chroms = [f"chr{i}" for i in range(1, 23)]
    header = [h for h in header if not h[0].startswith("##contig")]
    header[-1:-1] = [[f"##contig=<ID={c},length=250000000>"] for c in chroms]
    with gzip.open(path, "wt") as out:
        out.write("\n".join("\t".join(h) for h in header) + "\n")
        written, copy = 0, 0
        while written < n_records:
            chrom, shift = chroms[copy % len(chroms)], (copy // len(chroms)) * span
            for i, fields in enumerate(body):
                if written == n_records:
                    break
                fields = list(fields)
                fields[0] = chrom
                fields[1] = str(1 + shift + 100 * i)
                out.write("\t".join(fields) + "\n")
                written += 1
            copy += 1


def experiment(seq_id, aliquot, **extra):
    return {
        "seq_id": seq_id,
        "patient_id": seq_id,
        "aliquot": aliquot,
        "tenant_code": "mock",
        "family_role": "proband",
        "affected_status": "affected",
        "sex": "female",
        "experimental_strategy": "wgs",
        **extra,
    }


def run_one(flavour: str, engine: str, vcf: str) -> dict:
    sys.path.insert(0, str(ROOT))
    os.environ["RADIANT_SNV_ENGINE"] = engine
    import pyarrow.parquet as pq
    from pyiceberg.io.pyarrow import _get_parquet_writer_kwargs

    from radiant.tasks.iceberg.table_accumulator import TableAccumulator
    from radiant.tasks.vcf.experiment import RadiantGermlineAnnotationTask, RadiantSomaticAnnotationTask
    from radiant.tasks.vcf.snv import consequence, rust_engine, variant

    if flavour == "germline":
        from radiant.tasks.vcf.snv.germline import comphet, occurrence, process

        schemas = {"b.germline_snv_occurrence": occurrence.SCHEMA, "b.germline_snv_comphet": comphet.SCHEMA}
        task = RadiantGermlineAnnotationTask(
            task_id=1,
            part=1,
            analysis_type="germline",
            deleted=False,
            vcf_filepath=vcf,
            experiments=[
                experiment(1, "NA12878", family_role="proband", sex="Male"),
                experiment(2, "NA12891", family_role="father", sex="Male", affected_status="not_affected"),
                experiment(3, "NA12892", family_role="mother", sex="Female", affected_status="not_affected"),
            ],
        )
    else:
        from radiant.tasks.vcf.snv.somatic import occurrence, process

        schemas = {"b.somatic_snv_occurrence": occurrence.SCHEMA}
        task = RadiantSomaticAnnotationTask(
            task_id=1,
            part=1,
            analysis_type="somatic",
            deleted=False,
            vcf_filepath=vcf,
            experiments=[
                experiment(1, "TCR002361_SRX1091647-T", histology_type="tumoral"),
                experiment(2, "TCR002361_SRX1091646-N", histology_type="normal"),
            ],
        )
    schemas |= {"b.snv_variant": variant.SCHEMA, "b.snv_consequence": consequence.SCHEMA}
    rows = dict.fromkeys(schemas, 0)

    class Table:
        def __init__(self, name):
            self.n, self.catalog = name, None
            self.metadata = SimpleNamespace(properties={})

        def schema(self):
            return schemas[self.n]

        def name(self):
            return self.n

    class Catalog:
        def load_table(self, name):
            return Table(name)

    out_dir = tempfile.mkdtemp(prefix="bench-parquet-")
    counter = itertools.count()
    bytes_written = [0]

    def write_files(self, merge=True):
        # `_write_file` without the S3 part: the same pyarrow writer and options, to a local file.
        if merge:
            self.merge_table(force=True)
        if self.accumulated_pa_table is not None and self.accumulated_pa_table.num_rows:
            path = f"{out_dir}/{next(counter)}.parquet"
            with pq.ParquetWriter(path, self.accumulated_pa_table.schema, **_get_parquet_writer_kwargs({})) as writer:
                writer.write(self.accumulated_pa_table, row_group_size=1048576)
            rows[self.table.name()] += self.accumulated_pa_table.num_rows
            bytes_written[0] += os.path.getsize(path)
        self.accumulated_pa_table = None

    def upload_files(table, partition_filter, local_files):
        # The Rust engine already wrote the files locally; the S3 upload is left out.
        rows[table.name()] += sum(r for _, r, _ in local_files)
        bytes_written[0] += sum(b for _, _, b in local_files)
        return []

    process.load_catalog = lambda *a, **k: Catalog()
    TableAccumulator.write_files = write_files
    rust_engine.upload_files = upload_files
    start = time.perf_counter()
    process.process_task(task, namespace="b", vcf_threads=4)
    elapsed = time.perf_counter() - start
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_mb = rss / (1024 * 1024) if sys.platform == "darwin" else rss / 1024
    return {
        "engine": engine,
        "seconds": elapsed,
        "peak_rss_mb": rss_mb,
        "parquet_mb": bytes_written[0] / 1e6,
        "rows": rows,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--flavour", choices=list(TEMPLATES), default="germline")
    p.add_argument("--records", type=int, default=100_000)
    p.add_argument("--engine", choices=["python", "rust", "both"], default="both")
    p.add_argument("--_child", nargs=3, help=argparse.SUPPRESS)
    args = p.parse_args()
    if args._child:
        print(json.dumps(run_one(*args._child)))
        return
    with tempfile.TemporaryDirectory() as tmp:
        vcf = Path(tmp) / "bench.vcf.gz"
        build_vcf(TEMPLATES[args.flavour], vcf, args.records)
        engines = ["python", "rust"] if args.engine == "both" else [args.engine]
        results = {}
        for engine in engines:
            out = subprocess.run(
                [sys.executable, __file__, "--_child", args.flavour, engine, str(vcf)], capture_output=True, text=True
            )
            if out.returncode:
                sys.exit(f"{engine} failed:\n{out.stdout[-3000:]}\n{out.stderr[-3000:]}")
            results[engine] = json.loads(out.stdout.strip().splitlines()[-1])
        for r in results.values():
            print(
                f"{r['engine']:>6}: {r['seconds']:8.2f} s  {args.records / r['seconds']:10.0f} records/s  "
                f"peak RSS {r['peak_rss_mb']:7.0f} MB  parquet {r['parquet_mb']:6.1f} MB  rows {r['rows']}"
            )
        if len(results) == 2:
            print(f"speed-up: {results['python']['seconds'] / results['rust']['seconds']:.1f}x")
            if results["python"]["rows"] != results["rust"]["rows"]:
                sys.exit("row counts differ between engines")


if __name__ == "__main__":
    main()
