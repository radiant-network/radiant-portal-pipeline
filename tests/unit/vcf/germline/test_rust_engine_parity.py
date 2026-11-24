"""Parity of the Rust germline SNV engine with the Python one.

Both engines run through the real `process_task`, with a fake catalog. The Python rows are the Arrow table each
`TableAccumulator` would write; the Rust rows are its Parquet files, read back. They must be equal, row by row.

The VCFs are the unit test resources. Those without a CSQ header line (which `process_task` requires) get one added,
so the occurrence, zygosity, slivar and comphet files are covered too. Skipped when the extension is not built
(`maturin develop -m rust/radiant_vcf/Cargo.toml`).
"""

import os
import random
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

radiant_vcf = pytest.importorskip("radiant_vcf")

from radiant.tasks.iceberg.table_accumulator import TableAccumulator  # noqa: E402
from radiant.tasks.vcf.experiment import RadiantGermlineAnnotationTask  # noqa: E402
from radiant.tasks.vcf.snv import consequence, variant  # noqa: E402
from radiant.tasks.vcf.snv.germline import comphet, occurrence, rust_engine  # noqa: E402
from radiant.tasks.vcf.snv.germline import process as germline_process  # noqa: E402
from radiant.tasks.vcf.snv.germline.rust_engine import ENGINE_ENV  # noqa: E402

RESOURCES = Path(__file__).parent.parent.parent.parent / "resources"
GEN_TABLES = Path(__file__).parents[4] / "rust" / "radiant_vcf" / "gen_tables.py"

NAMESPACE = "mock"
SCHEMAS = {
    f"{NAMESPACE}.germline_snv_occurrence": occurrence.SCHEMA,
    f"{NAMESPACE}.snv_variant": variant.SCHEMA,
    f"{NAMESPACE}.snv_consequence": consequence.SCHEMA,
    f"{NAMESPACE}.germline_snv_comphet": comphet.SCHEMA,
}

CSQ_HEADER = (
    '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from Ensembl VEP. '
    'Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature|BIOTYPE|CANONICAL|PICK">\n'
)


class FakeTable:
    def __init__(self, name):
        self._name = name
        self.catalog = None
        # Default table properties: the writer options are PyIceberg's defaults.
        self.metadata = SimpleNamespace(properties={})

    def schema(self):
        return SCHEMAS[self._name]

    def name(self):
        return self._name


class FakeCatalog:
    def load_table(self, name):
        return FakeTable(name)


def run_engine(engine: str, vcf_path: str, experiments: list[dict], monkeypatch) -> dict[str, list[dict]]:
    """Run `process_task` with one engine; return the rows of each table.

    Python: the Arrow table each `TableAccumulator` would write. Rust: the Parquet files it wrote, read back, so
    the Rust writer (cast, nullability, compression) is under test too. Either must have the Iceberg schema.
    """
    captured = defaultdict(list)

    def write_files(self, merge=True):
        if merge:
            self.merge_table(force=True)
        if self.accumulated_pa_table is not None and self.accumulated_pa_table.num_rows:
            captured[self.table.name()].append(self.accumulated_pa_table)
        self.accumulated_pa_table = None

    def upload_files(table, partition_filter, local_files):
        # Read before the temporary directory goes away; one fake remote path per file.
        captured[table.name()].extend(pq.read_table(path) for path, _, _ in local_files)
        assert sum(rows for _, rows, _ in local_files) == sum(t.num_rows for t in captured[table.name()])
        return [f"s3://mock/{table.name()}/{i}.parquet" for i in range(len(local_files))]

    monkeypatch.setenv(ENGINE_ENV, engine)
    monkeypatch.setattr(germline_process, "load_catalog", lambda *a, **k: FakeCatalog())
    monkeypatch.setattr(TableAccumulator, "write_files", write_files)
    monkeypatch.setattr(rust_engine, "upload_files", upload_files)
    task = RadiantGermlineAnnotationTask(
        task_id=7,
        part=3,
        analysis_type="germline",
        experiments=experiments,
        deleted=False,
        vcf_filepath=vcf_path,
    )
    germline_process.process_task(task, namespace=NAMESPACE)
    out = {}
    for name in SCHEMAS:
        tables = captured.get(name, [])
        schema = TableAccumulator.remove_field_ids(SCHEMAS[name].as_arrow())
        table = pa.concat_tables(tables) if tables else schema.empty_table()
        assert table.schema == schema, f"{engine} {name}: schema differs from the Iceberg one"
        out[name] = table.to_pylist()
    return out


def experiment(seq_id, aliquot, role, affected="not_affected", sex="Female"):
    return {
        "seq_id": seq_id,
        "patient_id": seq_id,
        "aliquot": aliquot,
        "tenant_code": "mock_tenant",
        "family_role": role,
        "affected_status": affected,
        "sex": sex,
        "experimental_strategy": "wgs",
    }


def vcf_samples(path: Path) -> list[str]:
    for line in path.read_text().splitlines():
        if line.startswith("#CHROM"):
            return line.split("\t")[9:]
    return []


def pedigrees(samples: list[str]) -> list[tuple[str, list[dict]]]:
    """Several family setups over the samples of a file, including a subset of them."""
    s = samples
    if len(s) == 1:
        return [
            ("solo_male", [experiment(1, s[0], "proband", "affected", "Male")]),
            ("solo_female", [experiment(1, s[0], "proband", "affected", "Female")]),
        ]
    if len(s) == 2:
        return [
            ("duo_mother", [experiment(1, s[0], "proband", "affected", "Male"), experiment(2, s[1], "mother")]),
            ("duo_father", [experiment(1, s[0], "proband", "affected"), experiment(2, s[1], "father", "affected")]),
        ]
    setups = [
        (
            "trio_male_unaffected_parents",
            [
                experiment(1, s[0], "proband", "affected", "Male"),
                experiment(2, s[1], "mother", "not_affected", "Female"),
                experiment(3, s[2], "father", "not_affected", "Male"),
            ],
        ),
        (
            "trio_female_affected_father",
            [
                experiment(1, s[0], "proband", "affected", "Female"),
                experiment(2, s[1], "father", "affected", "Male"),
                experiment(3, s[2], "mother", "affected", "Female"),
            ],
        ),
        (
            "trio_proband_last",
            [
                experiment(1, s[2], "sibling", "affected", "Male"),
                experiment(2, s[0], "mother", "not_affected", "Female"),
                experiment(3, s[1], "father", "affected", "Male"),
            ],
        ),
        ("subset_proband_only", [experiment(1, s[0], "proband", "affected", "Male")]),
    ]
    if len(s) >= 4:
        setups[0][1].append(experiment(4, s[3], "brother", "affected", "Male"))
    return setups


def with_csq_header(src: Path, tmp_path: Path) -> Path:
    text = src.read_text()
    if "##INFO=<ID=CSQ" in text:
        return src
    lines = text.splitlines(keepends=True)
    idx = next(i for i, line in enumerate(lines) if line.startswith("#CHROM"))
    out = tmp_path / src.name
    out.write_text("".join(lines[:idx]) + CSQ_HEADER + "".join(lines[idx:]))
    return out


GERMLINE_VCFS = sorted(
    p.name for p in RESOURCES.glob("*.vcf") if vcf_samples(p) and "cnv" not in p.name and "somatic" not in p.name
) + ["integration/test.vcf"]

CASES = [
    pytest.param(name, setup, exps, id=f"{name}-{setup}")
    for name in GERMLINE_VCFS
    for setup, exps in pedigrees(vcf_samples(RESOURCES / name))
]


@pytest.mark.parametrize(("vcf_name", "setup", "experiments"), CASES)
def test_rust_engine_matches_python(vcf_name, setup, experiments, tmp_path, monkeypatch):
    path = str(with_csq_header(RESOURCES / vcf_name, tmp_path))
    try:
        python_rows = run_engine("python", path, experiments, monkeypatch)
    except Exception as python_error:
        # A file the Python path rejects must be rejected by the Rust one too.
        with pytest.raises((RuntimeError, ValueError, KeyError)):
            run_engine("rust", path, experiments, monkeypatch)
        pytest.skip(f"both engines reject the file (python: {python_error})")
    rust_rows = run_engine("rust", path, experiments, monkeypatch)
    for name in SCHEMAS:
        assert len(rust_rows[name]) == len(python_rows[name]), f"{name}: row count"
        for i, (r, p) in enumerate(zip(rust_rows[name], python_rows[name], strict=True)):
            assert r == p, f"{name} row {i}: " + ", ".join(
                f"{k}: rust={r[k]!r} python={p[k]!r}" for k in p if r[k] != p[k]
            )


def test_some_case_has_rows(monkeypatch):
    """Guard against a parity test that only ever compares empty tables, or never reaches the Rust engine."""
    calls = []
    original = germline_process.process_task_rust
    monkeypatch.setattr(germline_process, "process_task_rust", lambda *a, **k: calls.append(1) or original(*a, **k))
    path = str(RESOURCES / "test_consequence_merged.vcf")
    samples = vcf_samples(RESOURCES / "test_consequence_merged.vcf")
    rows = run_engine("rust", path, pedigrees(samples)[0][1], monkeypatch)
    assert calls == [1]
    assert all(rows[name] for name in SCHEMAS if "comphet" not in name)


def test_unknown_engine_raises(monkeypatch):
    monkeypatch.setenv(ENGINE_ENV, "go")
    with pytest.raises(ValueError, match="RADIANT_SNV_ENGINE"):
        run_engine("go", str(RESOURCES / "test_consequence_merged.vcf"), [], monkeypatch)


@pytest.mark.parametrize("vcf_name", ["test_occurrence_zygosity.vcf", "test_consequence_mane.vcf"])
def test_both_engines_raise_on_unsupported_input(vcf_name, monkeypatch):
    """No CSQ header line, or no sample at all: both engines refuse the file."""
    path = str(RESOURCES / vcf_name)
    exps = [experiment(1, "SA0001", "proband", "affected")]
    for engine in ("python", "rust"):
        with pytest.raises((KeyError, ValueError, RuntimeError)):
            run_engine(engine, path, exps, monkeypatch)


@pytest.mark.parametrize("slivar", [False, True], ids=["etl_inheritance", "slivar_inheritance"])
@pytest.mark.parametrize("setup", [0, 1, 2])
def test_synthetic_trio_parity(slivar, setup, tmp_path, monkeypatch):
    """Randomized genotypes, depths, INFO and slivar tags over a trio: the edge cases the resources do not reach."""
    path = tmp_path / "synthetic.vcf"
    write_synthetic_trio(path, n_records=3000, seed=42 + setup, slivar=slivar)
    exps = pedigrees(["P", "M", "F"])[setup][1]
    python_rows = run_engine("python", str(path), exps, monkeypatch)
    rust_rows = run_engine("rust", str(path), exps, monkeypatch)
    for name in SCHEMAS:
        assert python_rows[name], f"{name}: the synthetic file must produce rows"
        assert rust_rows[name] == python_rows[name], name


SLIVAR_TAGS = [
    "po_denovo",
    "po_mother",
    "po_father",
    "po_both",
    "autosomal_dominant_denovo",
    "autosomal_recessive",
    "x_linked_recessive",
    "unknown_father_genotype",
    "non_carrier_proband",
    "candidate",
]


def write_synthetic_trio(path: Path, n_records: int, seed: int, slivar: bool = False):
    rng = random.Random(seed)
    gts = ["0/0", "0/1", "1/1", "./.", "./1", "0/.", "0|1", "1|0", "1|1", "1", "0", ".", "1/0"]
    chroms = ["chr1", "chr2", "chrX", "chrY", "chrM", "chr22", "chrUn_KI270302v1"]
    csq_blocks = [
        "T|missense_variant|MODERATE|GENE1|ENSG00000001|ENST00000001|protein_coding|YES|1",
        "T|intron_variant&non_coding_transcript_variant|MODIFIER|GENE1|7157|NM_000546.6|protein_coding||",
        "T|upstream_gene_variant|LOW|GENE2||ENSR00000001|||",
        "T|stop_gained|HIGH|GENE3|ENSG00000003|XM_001.2|protein_coding|YES|",
    ]
    header = [
        "##fileformat=VCFv4.2",
        *[f"##contig=<ID={c},length=250000000>" for c in chroms],
        '##FILTER=<ID=LowQual,Description="x">',
        '##FILTER=<ID=VQSRTrancheSNP,Description="x">',
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="x">',
        '##FORMAT=<ID=AD,Number=R,Type=Integer,Description="x">',
        '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="x">',
        '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="x">',
        '##INFO=<ID=MQ,Number=1,Type=Float,Description="x">',
        '##INFO=<ID=DP,Number=1,Type=Integer,Description="x">',
        '##INFO=<ID=DS,Number=0,Type=Flag,Description="x">',
        '##INFO=<ID=culprit,Number=1,Type=String,Description="x">',
        '##INFO=<ID=InbreedCoeff,Number=1,Type=Float,Description="x">',
        '##INFO=<ID=MLEAC,Number=A,Type=Integer,Description="x">',
        '##INFO=<ID=slivar_comphet,Number=.,Type=String,Description="x">',
        CSQ_HEADER.strip(),
        *([f'##INFO=<ID={t},Number=.,Type=String,Description="x">' for t in SLIVAR_TAGS] if slivar else []),
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tP\tM\tF",
    ]
    lines = []
    pos = {c: 1000 for c in chroms}
    for _ in range(n_records):
        chrom = rng.choice(chroms)
        pos[chrom] += rng.randint(1, 500)
        ref, alt = rng.choice([("A", "T"), ("AC", "A"), ("G", "GTT"), ("C", "A,G")])
        info = []
        if rng.random() < 0.8:
            info.append(f"MQ={rng.uniform(10, 70):.2f}")
        if rng.random() < 0.5:
            info.append(f"DP={rng.randint(0, 300)}")
        if rng.random() < 0.3:
            info.append("DS")
        if rng.random() < 0.3:
            info.append(f"culprit={rng.choice(['MQ', 'FS'])}")
        if rng.random() < 0.5:
            info.append(f"InbreedCoeff={rng.uniform(-1, 1):.3f}")
        if rng.random() < 0.5 and "," not in alt:
            info.append(f"MLEAC={rng.randint(0, 6)}")
        if rng.random() < 0.2:
            entries = [
                f"{rng.choice(['P', 'M', 'F', 'X'])}/GENE{rng.randint(1, 3)}/{rng.randint(1, 9)}"
                f"/chr1/{rng.randint(1, 9999)}/A/T"
                for _ in range(rng.randint(1, 3))
            ]
            if rng.random() < 0.2:
                entries.append("malformed/entry")
            info.append("slivar_comphet=" + ",".join(entries))
        if slivar:
            for tag in rng.sample(SLIVAR_TAGS, rng.randint(0, 4)):
                info.append(f"{tag}=" + ",".join(rng.sample(["P", "M", "F", "fam1"], rng.randint(1, 3))))
        if rng.random() < 0.9:
            info.append("CSQ=" + ",".join(rng.sample(csq_blocks, rng.randint(1, len(csq_blocks)))))
        samples = []
        for _ in range(3):
            gt = rng.choice(gts)
            ad = rng.choice(
                [f"{rng.randint(0, 40)},{rng.randint(0, 40)}", ".", ".,.", f"{rng.randint(0, 5)},{rng.randint(0, 5)}"]
            )
            dp = rng.choice([str(rng.randint(0, 80)), "."])
            gq = rng.choice([str(rng.randint(0, 99)), "."])
            samples.append(f"{gt}:{ad}:{dp}:{gq}")
        qual = rng.choice([".", f"{rng.uniform(0, 5000):.2f}"])
        flt = rng.choice(["PASS", ".", "LowQual", "LowQual;VQSRTrancheSNP"])
        rid = rng.choice([".", f"rs{rng.randint(1, 10**8)}"])
        lines.append(
            "\t".join(
                [chrom, str(pos[chrom]), rid, ref, alt, qual, flt, ";".join(info) or ".", "GT:AD:DP:GQ", *samples]
            )
        )
    path.write_text("\n".join(header + lines) + "\n")


def test_tables_are_current():
    """src/tables.rs is generated from occurrence.py: regenerate it with gen_tables.py after a table change."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("gen_tables", GEN_TABLES)
    gen = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gen)
    assert gen.OUT.read_text() == gen.render(), "rust/radiant_vcf/src/tables.rs is stale: run gen_tables.py"


@pytest.fixture(autouse=True)
def _no_engine_env_leak():
    yield
    os.environ.pop(ENGINE_ENV, None)
