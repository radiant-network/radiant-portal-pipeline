"""The proband's slivar CNV set, read from the family VCF of a `radiant_germline_cnv_annotation` task."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from radiant.tasks.vcf.cnv.germline import process as cnv_process
from radiant.tasks.vcf.cnv.germline.occurrence import (
    ANY_ORIGIN,
    CNV_MODE_ORIGINS,
    CNV_MODE_TAGS,
    SCHEMA,
    is_carrier,
    slivar_transmission_mode,
)
from radiant.tasks.vcf.experiment import (
    RADIANT_GERMLINE_CNV_ANNOTATION_TASK,
    Experiment,
    RadiantGermlineCnvAnnotationTask,
    build_task_from_rows,
)
from radiant.tasks.vcf.snv.germline.inheritance import slivar_parental_origin
from tests.unit.vcf.vcf_test_utils import RESOURCES_DIR

TRIO_VCF = str(RESOURCES_DIR / "test_cnv_slivar.vcf")
NO_OID_VCF = str(RESOURCES_DIR / "test_cnv_slivar_no_oid.vcf")

# Aliquot -> (seq_id, family_role, affected_status)
MEMBERS = {
    "S_KID": (11, "proband", "affected"),
    "S_MOM": (12, "mother", "non_affected"),
    "S_DAD": (13, "father", "non_affected"),
    "S_SIB": (14, "sibling", "affected"),
}

NEW_COLUMNS = [f.name for f in SCHEMA.fields if f.field_id >= 125]
PARENT_FIELDS = ("calls", "cn", "bc", "pe", "sm", "name")


def _experiment(aliquot: str) -> Experiment:
    seq_id, role, affected = MEMBERS[aliquot]
    return Experiment(
        seq_id=seq_id,
        patient_id=seq_id,
        aliquot=aliquot,
        tenant_code="test",
        family_role=role,
        affected_status=affected,
        sex="male",
        experimental_strategy="wgs",
    )


def _task(filepath: str, aliquots: list[str], task_id: int = 9701) -> RadiantGermlineCnvAnnotationTask:
    return RadiantGermlineCnvAnnotationTask(
        task_id=task_id,
        part=1,
        analysis_type="germline",
        deleted=False,
        experiments=[_experiment(a) for a in aliquots],
        cnv_vcf_filepath=filepath,
    )


def _process(tasks) -> list[dict]:
    """Runs `process_tasks` against a mocked catalog and returns the rows it overwrites the table with, so the
    rows go through the Iceberg schema's Arrow conversion, as in production."""
    table = MagicMock()
    table.schema.return_value = SCHEMA
    catalog = MagicMock()
    catalog.load_table.return_value = table
    with patch.object(cnv_process, "load_catalog", return_value=catalog):
        cnv_process.process_tasks(tasks, namespace="test")
    (arrow_table,) = table.overwrite.call_args.args
    return arrow_table.to_pylist()


@pytest.fixture(scope="module")
def trio_rows() -> dict[str, dict]:
    rows = _process([_task(TRIO_VCF, ["S_MOM", "S_DAD", "S_KID", "S_SIB"])])
    return {r["name"]: r for r in rows}


def test_only_the_probands_carrier_records_give_rows(trio_rows):
    """`0/0` and `./.` records of the proband are a relative's CNVs: no row, and no row for the relatives."""
    assert sorted(trio_rows) == sorted(
        [
            "DRAGEN:GAIN:chr1:1001-5000",
            "DRAGEN:LOSS:chr1:10001-20000",
            "DRAGEN:GAIN:chr1:50001-60000",
            "DRAGEN:GAIN:chr1:70001-80000",
            "DRAGEN:LOSS:chr1:90001-99000",
            "DRAGEN:LOSS:chrX:100001-120000",
        ]
    )
    assert {(r["seq_id"], r["aliquot"], r["task_id"], r["tenant_code"]) for r in trio_rows.values()} == {
        (11, "S_KID", 9701, "test")
    }


def test_proband_row_keeps_the_slivar_record_fields(trio_rows):
    row = trio_rows["DRAGEN:LOSS:chr1:10001-20000"]
    assert (row["chromosome"], row["start"], row["end"], row["type"], row["alternate"]) == (
        "1",
        10000,
        20000,
        "LOSS",
        "<DEL>",
    )
    assert (row["calls"], row["cn"], row["bc"], row["pe"]) == ([0, 1], 1, 9, [13, 14])
    assert row["sm"] == pytest.approx(0.49)


def test_parent_fields_come_from_the_same_record(trio_rows):
    row = trio_rows["DRAGEN:LOSS:chr1:10001-20000"]
    assert (row["mother_calls"], row["mother_cn"], row["mother_bc"], row["mother_pe"]) == ([0, 1], 1, 9, [11, 12])
    assert row["mother_sm"] == pytest.approx(0.52)
    assert (row["father_calls"], row["father_cn"], row["father_bc"], row["father_pe"]) == ([0, 0], 2, 9, [0, 0])
    assert row["father_sm"] == pytest.approx(1.01)


def test_parent_names_are_their_own_decoded_dragen_ids(trio_rows):
    """`FORMAT/OID` is percent-encoded (`%3A`); the column holds the DRAGEN ID, as `name` on the parent's
    alignment-task row. A parent with no call of its own in the record has `.`: no name."""
    row = trio_rows["DRAGEN:LOSS:chr1:10001-20000"]
    assert row["mother_name"] == "DRAGEN:LOSS:chr1:10001-20000"
    assert row["father_name"] is None


def test_missing_parent_values_are_null(trio_rows):
    """A `./.` parent with empty FORMAT values: calls kept as no-call, every number NULL, never INT32 min."""
    row = trio_rows["DRAGEN:GAIN:chr1:50001-60000"]
    assert row["father_calls"] == [-1, -1]
    assert (row["father_cn"], row["father_bc"], row["father_pe"], row["father_sm"], row["father_name"]) == (
        None,
        None,
        None,
        None,
        None,
    )
    assert row["mother_cn"] == 2


@pytest.mark.parametrize(
    "name,origin,mode",
    [
        ("DRAGEN:GAIN:chr1:1001-5000", "DENOVO", "denovo"),
        ("DRAGEN:LOSS:chr1:10001-20000", "MOTHER", "inherited_maternal"),
        ("DRAGEN:GAIN:chr1:50001-60000", "POSSIBLE_DENOVO", "possible_denovo"),
        ("DRAGEN:LOSS:chrX:100001-120000", "DENOVO", "x_linked_denovo"),
    ],
)
def test_origin_and_mode_from_the_probands_tags(trio_rows, name, origin, mode):
    assert (trio_rows[name]["parental_origin"], trio_rows[name]["transmission_mode"]) == (origin, mode)


def test_a_siblings_confirmed_tag_is_not_the_probands(trio_rows):
    """slivar lists every affected child under the sibling's `denovo`, the proband too. The proband's own
    `po_possible_denovo` does not pair with `denovo`: the proband keeps its own uncertain value."""
    row = trio_rows["DRAGEN:LOSS:chr1:90001-99000"]
    assert (row["parental_origin"], row["transmission_mode"]) == ("POSSIBLE_DENOVO", "possible_denovo")


def test_record_with_no_tag_keeps_origin_and_mode_null(trio_rows):
    row = trio_rows["DRAGEN:GAIN:chr1:70001-80000"]
    assert (row["parental_origin"], row["transmission_mode"]) == (None, None)
    assert row["mother_calls"] == [0, 0]


def test_haploid_proband_call_is_a_carrier(trio_rows):
    row = trio_rows["DRAGEN:LOSS:chrX:100001-120000"]
    assert (row["calls"], row["cn"], row["father_calls"]) == ([1], 0, [0])


def test_vcf_without_original_ids_gives_null_parent_names():
    """A slivar VCF made before FORMAT/OID (BIOINFO-214): the mother's calls load, her name stays NULL. A duo has no
    father: his columns stay NULL."""
    rows = {r["name"]: r for r in _process([_task(NO_OID_VCF, ["S_MOM", "S_KID"])])}
    assert sorted(rows) == ["DRAGEN:GAIN:chr1:50001-60000", "DRAGEN:LOSS:chr1:10001-20000"]
    row = rows["DRAGEN:LOSS:chr1:10001-20000"]
    assert (row["mother_calls"], row["mother_cn"], row["mother_name"]) == ([0, 1], 1, None)
    assert all(row[f"father_{field}"] is None for field in PARENT_FIELDS)
    assert (row["parental_origin"], row["transmission_mode"]) == ("MOTHER", "inherited_maternal")


def test_singleton_rows_have_no_parent_value():
    """Only the proband in the task: the parents' samples are not read, every parent column stays NULL."""
    rows = _process([_task(TRIO_VCF, ["S_KID"])])
    assert rows
    assert all(r[f"{role}_{field}"] is None for r in rows for role in ("mother", "father") for field in PARENT_FIELDS)


def test_alignment_task_rows_keep_the_new_columns_null():
    from radiant.tasks.vcf.experiment import AlignmentGermlineVariantCallingTask

    task = AlignmentGermlineVariantCallingTask(
        task_id=9702,
        part=1,
        analysis_type="germline",
        deleted=False,
        experiments=[_experiment("S_KID").model_copy(update={"aliquot": "SA0001"})],
        cnv_vcf_filepath=str(RESOURCES_DIR / "integration" / "test_cnv.vcf"),
    )
    rows = _process([task])
    assert rows
    assert all(r[c] is None for r in rows for c in NEW_COLUMNS)


def test_task_without_proband_in_the_vcf_fails():
    task = _task(TRIO_VCF, ["S_MOM", "S_DAD", "S_KID"]).model_copy(
        update={"experiments": [_experiment("S_MOM"), _experiment("S_DAD")]}
    )
    with pytest.raises(ValueError, match="no proband sample"):
        _process([task])


@pytest.mark.parametrize(
    "tag,origin",
    [
        (tag, "DENOVO" if origins is ANY_ORIGIN else sorted(origins, key=str)[0])
        for tag, origins in CNV_MODE_ORIGINS.items()
    ],
)
def test_each_mode_tag_is_stored_under_its_own_name(tag, origin):
    record = SimpleNamespace(INFO={tag: "S_KID"})
    assert slivar_transmission_mode(record, "S_KID", origin) == tag


# (mode tag, an origin its TRIO_TABLE row never gives): the tag must then come from another child.
@pytest.mark.parametrize(
    "tag,origin",
    [
        ("denovo", "MOTHER"),
        ("denovo", None),
        ("inherited_maternal", "FATHER"),
        ("inherited_paternal", "POSSIBLE_FATHER"),
        ("possible_inherited_maternal", "MOTHER"),
        ("recessive", "MOTHER"),
        ("recessive_lowconf", "BOTH"),
        ("x_linked_denovo", "POSSIBLE_DENOVO"),
        ("mendelian_inconsistent", "AMBIGUOUS"),
    ],
)
def test_a_tag_that_does_not_pair_with_the_probands_origin_is_dropped(tag, origin):
    record = SimpleNamespace(INFO={tag: "S_KID,S_SIB"})
    assert slivar_transmission_mode(record, "S_KID", origin) is None


def test_mode_origins_follow_the_rule_table():
    """cnv-post-processing @ cad99b4, assets/cnv-slivar-functions.js: TRIO_TABLE columns 3 and 4, the X-linked
    renames of `denovo` and `recessive`, the catch-all row (no origin), and `unknown_cn` before the table."""
    assert CNV_MODE_ORIGINS["recessive_lowconf"] == {"MOTHER", "FATHER"}
    assert CNV_MODE_ORIGINS["inheritance_unresolved"] == {"BOTH", "AMBIGUOUS", None}
    assert CNV_MODE_ORIGINS["x_linked_denovo"] == CNV_MODE_ORIGINS["denovo"]
    assert CNV_MODE_ORIGINS["x_linked_recessive"] == CNV_MODE_ORIGINS["recessive"]
    assert CNV_MODE_ORIGINS["unknown_cn"] is ANY_ORIGIN


def test_two_tags_that_both_pair_with_the_origin_give_no_mode():
    """The sibling's `denovo` and the proband's `denovo_suspect` both come from `po_denovo` rows: no tag is guessed,
    and the proband is never shown a confirmed de novo that may be the sibling's."""
    record = SimpleNamespace(INFO={"denovo": "S_KID,S_SIB", "denovo_suspect": "S_KID,S_SIB"})
    assert slivar_transmission_mode(record, "S_KID", "DENOVO") is None


def test_unknown_cn_from_a_sibling_hides_the_probands_mode():
    """`unknown_cn` pairs with any origin, so it can never be ruled out."""
    record = SimpleNamespace(INFO={"inherited_maternal": "S_KID,S_SIB", "unknown_cn": "S_KID,S_SIB"})
    assert slivar_transmission_mode(record, "S_KID", "MOTHER") is None


def test_mode_tags_are_the_pipelines_fourteen():
    """cnv-post-processing @ cad99b4, conf/modules.config `--family-expr`."""
    assert len(CNV_MODE_TAGS) == len(set(CNV_MODE_TAGS)) == 14


@pytest.mark.parametrize(
    "tag,origin",
    [
        ("po_denovo", "DENOVO"),
        ("po_mother", "MOTHER"),
        ("po_father", "FATHER"),
        ("po_both", "BOTH"),
        ("po_ambiguous", "AMBIGUOUS"),
        ("po_possible_denovo", "POSSIBLE_DENOVO"),
        ("po_possible_mother", "POSSIBLE_MOTHER"),
        ("po_possible_father", "POSSIBLE_FATHER"),
    ],
)
def test_each_po_tag_maps_to_the_snv_value(tag, origin):
    record = SimpleNamespace(INFO={tag: "S_KID"})
    assert slivar_parental_origin(record, "S_KID") == origin


@pytest.mark.parametrize(
    "tag,origin",
    [("possible_denovo", "POSSIBLE_DENOVO"), ("possible_inherited_maternal", "POSSIBLE_MOTHER")],
)
def test_an_uncertain_tag_never_gives_a_confirmed_value(tag, origin):
    """The sibling's `denovo` lists the proband too, as slivar writes a family tag."""
    record = SimpleNamespace(INFO={tag: "S_KID,S_SIB", "denovo": "S_KID,S_SIB"})
    assert slivar_transmission_mode(record, "S_KID", origin) == tag


def test_unaffected_proband_gets_an_origin_and_no_mode():
    """Mode tags list the affected members only; the `po_*` tags list every child."""
    record = SimpleNamespace(INFO={"po_mother": "S_KID", "inherited_maternal": "S_MOM"})
    assert slivar_parental_origin(record, "S_KID") == "MOTHER"
    assert slivar_transmission_mode(record, "S_KID", "MOTHER") is None


@pytest.mark.parametrize(
    "calls,carrier", [([0, 1], True), ([1, 1], True), ([-1, 1], True), ([1], True), ([0, 0], False), ([-1, -1], False)]
)
def test_is_carrier(calls, carrier):
    assert is_carrier(calls) is carrier


def _row(aliquot: str, filepath: str = "s3://test-vcf/mock-9701.cnv.slivar.vcf.gz") -> dict:
    seq_id, role, affected = MEMBERS[aliquot]
    return {
        "task_id": 9701,
        "part": 1,
        "task_type": RADIANT_GERMLINE_CNV_ANNOTATION_TASK,
        "analysis_type": "germline",
        "deleted": False,
        "seq_id": seq_id,
        "patient_id": seq_id,
        "aliquot": aliquot,
        "tenant_code": "test",
        "family_role": role,
        "affected_status": affected,
        "sex": "male",
        "experimental_strategy": "wgs",
        "cnv_vcf_filepath": filepath,
    }


def test_task_is_built_from_every_members_row():
    task = build_task_from_rows([_row("S_KID"), _row("S_MOM"), _row("S_DAD")])
    assert isinstance(task, RadiantGermlineCnvAnnotationTask)
    assert task.cnv_vcf_filepath == "s3://test-vcf/mock-9701.cnv.slivar.vcf.gz"
    assert sorted(e.family_role for e in task.experiments) == ["father", "mother", "proband"]


def test_task_without_proband_row_fails():
    with pytest.raises(ValueError, match="No proband"):
        build_task_from_rows([_row("S_MOM"), _row("S_DAD")])


def test_task_with_two_slivar_files_fails():
    with pytest.raises(ValueError, match="one slivar VCF"):
        build_task_from_rows([_row("S_KID"), _row("S_MOM", "s3://test-vcf/other.cnv.slivar.vcf.gz")])
