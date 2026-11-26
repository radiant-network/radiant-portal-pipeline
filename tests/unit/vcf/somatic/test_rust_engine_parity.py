"""Parity of the Rust somatic SNV engine with the Python one.

Both engines run through the real somatic `process_task`, with a fake catalog. The Python rows are the Arrow table
each `TableAccumulator` would write; the Rust rows are its Parquet files, read back. They must be equal, row by row
(a NaN equals a NaN: `FORMAT/SQ` passes missing values through as NaN). Skipped when the extension is not built
(`maturin develop -m rust/radiant_vcf/Cargo.toml`).
"""

import math
import random
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

radiant_vcf = pytest.importorskip("radiant_vcf")

from radiant.tasks.iceberg.table_accumulator import TableAccumulator  # noqa: E402
from radiant.tasks.vcf.experiment import RadiantSomaticAnnotationTask  # noqa: E402
from radiant.tasks.vcf.snv import consequence, rust_engine, variant  # noqa: E402
from radiant.tasks.vcf.snv.rust_engine import ENGINE_ENV  # noqa: E402
from radiant.tasks.vcf.snv.somatic import occurrence  # noqa: E402
from radiant.tasks.vcf.snv.somatic import process as somatic_process  # noqa: E402

RESOURCES = Path(__file__).parents[3] / "resources"
TUMOR_NORMAL_VCF = RESOURCES / "test_somatic_snv.vcf"
TUMOR_ONLY_VCF = RESOURCES / "test_somatic_snv_tumor_only.vcf"
NORMAL = "TCR002361_SRX1091646-N"
TUMOR = "TCR002361_SRX1091647-T"

NAMESPACE = "mock"
SCHEMAS = {
    f"{NAMESPACE}.somatic_snv_occurrence": occurrence.SCHEMA,
    f"{NAMESPACE}.snv_variant": variant.SCHEMA,
    f"{NAMESPACE}.snv_consequence": consequence.SCHEMA,
}


class FakeTable:
    def __init__(self, name):
        self._name = name
        self.catalog = None
        self.metadata = SimpleNamespace(properties={})

    def schema(self):
        return SCHEMAS[self._name]

    def name(self):
        return self._name


class FakeCatalog:
    def load_table(self, name):
        return FakeTable(name)


def run_engine(engine: str, vcf_path: str, experiments: list[dict], monkeypatch) -> dict[str, list[dict]]:
    """Run the somatic `process_task` with one engine; return the rows of each table (see the module docstring)."""
    captured = defaultdict(list)

    def write_files(self, merge=True):
        if merge:
            self.merge_table(force=True)
        if self.accumulated_pa_table is not None and self.accumulated_pa_table.num_rows:
            captured[self.table.name()].append(self.accumulated_pa_table)
        self.accumulated_pa_table = None

    def upload_files(table, partition_filter, local_files):
        captured[table.name()].extend(pq.read_table(path) for path, _, _ in local_files)
        return [f"s3://mock/{table.name()}/{i}.parquet" for i in range(len(local_files))]

    monkeypatch.setenv(ENGINE_ENV, engine)
    monkeypatch.setattr(somatic_process, "load_catalog", lambda *a, **k: FakeCatalog())
    monkeypatch.setattr(TableAccumulator, "write_files", write_files)
    monkeypatch.setattr(rust_engine, "upload_files", upload_files)
    task = RadiantSomaticAnnotationTask(
        task_id=7,
        part=3,
        analysis_type="somatic",
        experiments=experiments,
        deleted=False,
        vcf_filepath=vcf_path,
    )
    result = somatic_process.process_task(task, namespace=NAMESPACE)
    assert set(result) == set(SCHEMAS)
    out = {}
    for name in SCHEMAS:
        tables = captured.get(name, [])
        schema = TableAccumulator.remove_field_ids(SCHEMAS[name].as_arrow())
        table = pa.concat_tables(tables) if tables else schema.empty_table()
        assert table.schema == schema, f"{engine} {name}: schema differs from the Iceberg one"
        out[name] = table.to_pylist()
    return out


def same(a, b) -> bool:
    """Equality where a NaN equals a NaN."""
    if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
        return True
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b, strict=True))
    return a == b


def assert_same(python_rows, rust_rows):
    for name in SCHEMAS:
        p, r = python_rows[name], rust_rows[name]
        assert len(r) == len(p), f"{name}: row count"
        for i, (rr, pr) in enumerate(zip(r, p, strict=True)):
            assert same(rr, pr), f"{name} row {i}: " + ", ".join(
                f"{k}: rust={rr[k]!r} python={pr[k]!r}" for k in pr if not same(rr[k], pr[k])
            )


def experiment(seq_id, aliquot, histology):
    return {
        "seq_id": seq_id,
        "patient_id": seq_id,
        "aliquot": aliquot,
        "tenant_code": "mock_tenant",
        "family_role": "proband",
        "affected_status": "affected",
        "sex": "female",
        "experimental_strategy": "wgs",
        "histology_type": histology,
    }


CASES = {
    # The normal is the first sample of the file, the tumor the second.
    "tumor_normal": (TUMOR_NORMAL_VCF, [experiment(1, TUMOR, "tumoral"), experiment(2, NORMAL, "normal")]),
    "tumor_normal_listed_normal_first": (
        TUMOR_NORMAL_VCF,
        [experiment(2, NORMAL, "normal"), experiment(1, TUMOR, "tumoral")],
    ),
    "tumor_only": (TUMOR_ONLY_VCF, [experiment(1, TUMOR, "tumoral")]),
}


@pytest.mark.parametrize("case", list(CASES))
def test_rust_engine_matches_python(case, monkeypatch):
    path, exps = CASES[case]
    python_rows = run_engine("python", str(path), exps, monkeypatch)
    rust_rows = run_engine("rust", str(path), exps, monkeypatch)
    assert python_rows[f"{NAMESPACE}.somatic_snv_occurrence"], "the case must produce rows"
    assert_same(python_rows, rust_rows)


@pytest.mark.parametrize(
    ("path", "exps", "error", "match"),
    [
        pytest.param(
            TUMOR_NORMAL_VCF, [experiment(1, "OTHER", "tumoral")], ValueError, "no matching samples", id="none"
        ),
        pytest.param(
            TUMOR_ONLY_VCF,
            [experiment(1, TUMOR, "tumoral"), experiment(2, NORMAL, "normal")],
            ValueError,
            "absent from",
            id="missing_normal",
        ),
        pytest.param(
            TUMOR_NORMAL_VCF,
            [experiment(1, TUMOR, "tumoral")],
            ValueError,
            "resolved to a single",
            id="phantom_tumor_only",
        ),
        pytest.param(
            TUMOR_ONLY_VCF, [experiment(1, TUMOR, "normal")], ValueError, "expected 'tumoral'", id="normal_only"
        ),
        pytest.param(
            TUMOR_ONLY_VCF, [experiment(1, TUMOR, "metastasis")], ValueError, "valid histology", id="bad_histology"
        ),
    ],
)
def test_both_engines_reject_the_same_tasks(path, exps, error, match, monkeypatch):
    for engine in ("python", "rust"):
        with pytest.raises(error, match=match):
            run_engine(engine, str(path), exps, monkeypatch)


def test_both_engines_need_a_csq_line(tmp_path, monkeypatch):
    path = tmp_path / "no_csq.vcf"
    path.write_text("".join(line for line in TUMOR_ONLY_VCF.open() if not line.startswith("##INFO=<ID=CSQ")))
    for engine in ("python", "rust"):
        with pytest.raises(KeyError):
            run_engine(engine, str(path), [experiment(1, TUMOR, "tumoral")], monkeypatch)


@pytest.mark.parametrize("tumor_only", [False, True], ids=["tumor_normal", "tumor_only"])
def test_synthetic_parity(tumor_only, tmp_path, monkeypatch):
    """Randomized genotypes, depths, SQ, hotspot tags and somatic INFO: the cases the resources do not reach."""
    path = tmp_path / "synthetic.vcf"
    write_synthetic(path, n_records=3000, seed=7, tumor_only=tumor_only)
    exps = (
        [experiment(1, "T", "tumoral")]
        if tumor_only
        else [experiment(1, "T", "tumoral"), experiment(2, "N", "normal")]
    )
    python_rows = run_engine("python", str(path), exps, monkeypatch)
    rust_rows = run_engine("rust", str(path), exps, monkeypatch)
    for name in SCHEMAS:
        assert python_rows[name], f"{name}: the synthetic file must produce rows"
    assert_same(python_rows, rust_rows)


CSQ_HEADER = (
    '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from Ensembl VEP. '
    'Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature|BIOTYPE|CANONICAL|PICK">'
)


def write_synthetic(path: Path, n_records: int, seed: int, tumor_only: bool):
    rng = random.Random(seed)
    samples = ["T"] if tumor_only else ["N", "T"]
    gts = ["0/0", "0/1", "1/1", "./.", "./1", "0|1", "1", "0", "."]
    chroms = ["chr1", "chr7", "chrX", "chrY", "chrM", "chrUn_KI270302v1"]
    csq_blocks = [
        "T|missense_variant|MODERATE|EGFR|ENSG00000146648|ENST00000275493|protein_coding|YES|1",
        "T|intron_variant|MODIFIER|EGFR|1956|NM_005228.5|protein_coding||",
        "T|stop_gained|HIGH|TP53|ENSG00000141510|ENST00000269305|protein_coding|YES|",
    ]
    header = [
        "##fileformat=VCFv4.2",
        *[f"##contig=<ID={c},length=250000000>" for c in chroms],
        '##FILTER=<ID=germline,Description="x">',
        '##FILTER=<ID=haplotype,Description="x">',
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="x">',
        '##FORMAT=<ID=AD,Number=R,Type=Integer,Description="x">',
        '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="x">',
        '##FORMAT=<ID=SQ,Number=A,Type=Float,Description="x">',
        '##INFO=<ID=DP,Number=1,Type=Integer,Description="x">',
        '##INFO=<ID=MQ0,Number=1,Type=Float,Description="x">',
        '##INFO=<ID=GERMQ,Number=1,Type=Integer,Description="x">',
        '##INFO=<ID=TLOD,Number=A,Type=Float,Description="x">',
        '##INFO=<ID=MAPQ,Number=1,Type=Float,Description="x">',
        '##INFO=<ID=AQ,Number=A,Type=Float,Description="x">',
        '##INFO=<ID=HotspotAllele,Number=1,Type=Integer,Description="x">',
        '##INFO=<ID=hotspot,Number=0,Type=Flag,Description="x">',
        '##INFO=<ID=culprit,Number=1,Type=String,Description="x">',
        CSQ_HEADER,
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t" + "\t".join(samples),
    ]
    lines = []
    pos = dict.fromkeys(chroms, 1000)
    for _ in range(n_records):
        chrom = rng.choice(chroms)
        pos[chrom] += rng.randint(1, 500)
        ref, alt = rng.choice([("A", "T"), ("AC", "A"), ("G", "GTT"), ("C", "A,G")])
        info = []
        for tag, value in [
            ("DP", lambda: rng.randint(0, 300)),
            ("MQ0", lambda: f"{rng.uniform(0, 5):.2f}"),
            ("GERMQ", lambda: rng.randint(0, 90)),
            ("TLOD", lambda: f"{rng.uniform(0, 50):.2f}"),
            ("MAPQ", lambda: f"{rng.uniform(0, 60):.1f}"),
            ("AQ", lambda: f"{rng.uniform(0, 30):.1f}"),
            ("HotspotAllele", lambda: rng.choice([0, 1, 2])),
            ("culprit", lambda: rng.choice(["MQ", "FS"])),
        ]:
            if rng.random() < 0.5 and not (tag in ("TLOD", "AQ") and "," in alt):
                info.append(f"{tag}={value()}")
        if rng.random() < 0.3:
            info.append("hotspot")
        if rng.random() < 0.9:
            info.append("CSQ=" + ",".join(rng.sample(csq_blocks, rng.randint(1, len(csq_blocks)))))
        fields = []
        for _ in samples:
            gt = rng.choice(gts)
            ad = rng.choice(
                [f"{rng.randint(0, 40)},{rng.randint(0, 40)}", ".", f"{rng.randint(0, 3)},{rng.randint(0, 3)}"]
            )
            dp = rng.choice([str(rng.randint(0, 80)), "."])
            sq = rng.choice([f"{rng.uniform(0, 40):.2f}", "."])
            fields.append(f"{gt}:{ad}:{dp}:{sq}" if "," not in alt else f"{gt}:{ad},0:{dp}:{sq},0")
        qual = rng.choice([".", f"{rng.uniform(0, 500):.2f}"])
        flt = rng.choice(["PASS", ".", "germline", "germline;haplotype"])
        lines.append(
            "\t".join(
                [
                    chrom,
                    str(pos[chrom]),
                    rng.choice([".", "rs99"]),
                    ref,
                    alt,
                    qual,
                    flt,
                    ";".join(info) or ".",
                    "GT:AD:DP:SQ",
                ]
                + fields
            )
        )
    path.write_text("\n".join(header + lines) + "\n")
