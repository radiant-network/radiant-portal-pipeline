import logging

from radiant.tasks.vcf.experiment import Experiment, RadiantGermlineAnnotationTask
from radiant.tasks.vcf.pedigree import Pedigree
from radiant.tasks.vcf.snv.common import locus_and_hash, process_common
from radiant.tasks.vcf.snv.germline.comphet import process_comphet
from tests.unit.vcf.vcf_test_utils import variant

VCF_FILE = "test_comphet.vcf"
SAMPLES = ["SA0001", "SA0002", "SA0003"]


def _experiment(seq_id, aliquot, family_role, affected_status="not_affected"):
    return Experiment(
        seq_id=seq_id,
        patient_id=seq_id,
        aliquot=aliquot,
        tenant_code="tenant1",
        family_role=family_role,
        affected_status=affected_status,
        sex="F",
        experimental_strategy="wgs",
        request_priority="routine",
    )


def _pedigree(*experiments):
    task = RadiantGermlineAnnotationTask(
        task_id=7,
        part=3,
        analysis_type="germline",
        deleted=False,
        experiments=list(experiments),
        vcf_filepath="",
    )
    return Pedigree(task, SAMPLES)


TRIO = _pedigree(
    _experiment(1, "SA0001", "proband", "affected"),
    _experiment(2, "SA0002", "mother"),
    _experiment(3, "SA0003", "father"),
)


def _comphets(row_number, ped=TRIO):
    record = variant(VCF_FILE, row_number=row_number)
    return process_comphet(record, ped, process_common(record, task_id=7, part=3))


def _hash(pos, ref, alt):
    return locus_and_hash("1", pos, ref, alt)[1]


def test_one_pair():
    assert _comphets(1) == [
        {
            "task_id": 7,
            "locus_hash": _hash(1000, "A", "G"),
            "part": 3,
            "seq_id": 1,
            "tenant_code": "tenant1",
            "partner_locus_hash": _hash(2000, "C", "T"),
            "symbol": "GENE1",
        }
    ]


def test_partner_hash_matches_the_partner_record():
    partner = variant(VCF_FILE, row_number=2)
    assert _comphets(1)[0]["partner_locus_hash"] == process_common(partner, task_id=7, part=3).locus_hash


def test_both_directions_of_a_pair():
    first = _comphets(1)[0]
    reverse = next(c for c in _comphets(2) if c["symbol"] == "GENE1")
    assert (reverse["locus_hash"], reverse["partner_locus_hash"]) == (
        first["partner_locus_hash"],
        first["locus_hash"],
    )


def test_one_variant_paired_in_two_genes_keeps_progeny_entries_only(caplog):
    with caplog.at_level(logging.WARNING, logger="airflow.task"):
        comphets = _comphets(2)

    # The duplicated pair id 5, the mother's entry, the unknown sample and the malformed entry are dropped.
    assert sorted((c["seq_id"], c["partner_locus_hash"], c["symbol"]) for c in comphets) == sorted(
        [
            (1, _hash(1000, "A", "G"), "GENE1"),
            (1, _hash(3000, "G", "A"), "GENE2"),
        ]
    )
    assert "SA0001/GENE1/broken" in caplog.text


def test_no_tag():
    assert _comphets(4) == []


def test_solo():
    solo = _pedigree(_experiment(1, "SA0001", "proband", "affected"))
    assert [c["seq_id"] for c in _comphets(1, solo)] == [1]


def test_duo_with_a_sibling_progeny():
    duo = _pedigree(_experiment(1, "SA0001", "sister", "affected"), _experiment(2, "SA0002", "mother"))
    assert [c["seq_id"] for c in _comphets(1, duo)] == [1]


def test_duo_with_a_sibling_role_progeny():
    duo = _pedigree(_experiment(1, "SA0001", "sibling", "affected"), _experiment(2, "SA0002", "mother"))
    assert [c["seq_id"] for c in _comphets(1, duo)] == [1]


def test_entries_of_a_sample_outside_the_task_are_ignored():
    other_proband = _pedigree(_experiment(2, "SA0002", "proband", "affected"))
    assert [(c["seq_id"], c["symbol"]) for c in _comphets(2, other_proband)] == [(2, "GENE1")]
