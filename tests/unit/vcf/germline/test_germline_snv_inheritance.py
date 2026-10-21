from radiant.tasks.vcf.experiment import Experiment, RadiantGermlineAnnotationTask
from radiant.tasks.vcf.pedigree import Pedigree
from radiant.tasks.vcf.snv.common import process_common
from radiant.tasks.vcf.snv.germline.inheritance import (
    SlivarMethods,
    slivar_inheritance_methods,
    slivar_parental_origin,
    slivar_transmission_mode,
)
from radiant.tasks.vcf.snv.germline.occurrence import process_occurrence
from tests.unit.vcf.vcf_test_utils import variant, vcf

SLIVAR_VCF = "test_inheritance_slivar.vcf"
OLD_SLIVAR_VCF = "test_inheritance_old_slivar.vcf"
DE_NOVO_SLIVAR_VCF = "test_inheritance_de_novo_slivar.vcf"
NO_TAG_VCF = "test_comphet.vcf"
SAMPLES = ["SA0001", "SA0002", "SA0003"]
BOTH = SlivarMethods(parental_origin=True, transmission_mode=True)


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
SOLO = _pedigree(_experiment(1, "SA0001", "proband", "affected"))


def _mode(row_number):
    return slivar_transmission_mode(variant(SLIVAR_VCF, row_number=row_number), "SA0001")


def _origin(row_number):
    return slivar_parental_origin(variant(SLIVAR_VCF, row_number=row_number), "SA0001")


def _occurrences(vcf_file, row_number, ped=TRIO, slivar=BOTH):
    record = variant(vcf_file, row_number=row_number)
    return process_occurrence(record, ped, process_common(record, task_id=7, part=3), slivar=slivar)


def _methods(vcf_file):
    with vcf(vcf_file) as v:
        return slivar_inheritance_methods(v)


def test_methods_slivar_vcf():
    assert _methods(SLIVAR_VCF) == SlivarMethods(parental_origin=True, transmission_mode=True)


def test_methods_old_tag_names_keep_the_etl_mode():
    assert _methods(OLD_SLIVAR_VCF) == SlivarMethods(parental_origin=True, transmission_mode=False)


def test_methods_de_novo_tag_names_keep_the_etl_mode():
    assert _methods(DE_NOVO_SLIVAR_VCF) == SlivarMethods(parental_origin=True, transmission_mode=False)


def test_methods_vcf_without_inheritance_tags():
    assert _methods(NO_TAG_VCF) == SlivarMethods(parental_origin=False, transmission_mode=False)


def test_de_novo():
    assert _mode(1) == ("autosomal_dominant_de_novo", [], False)
    assert _origin(1) == "DENOVO"


def test_x_linked_de_novo_tags_map_to_radiant_values():
    assert _mode(10) == ("x_linked_dominant_de_novo", ["x_linked_recessive_de_novo"], False)


def test_recessive_wins_over_dominant():
    assert _mode(2) == ("autosomal_recessive", ["autosomal_dominant"], False)
    assert _origin(2) == "BOTH"


def test_uniparental_disomy_alone_is_kept():
    assert _mode(3) == ("uniparental_disomy", [], False)


def test_dominant_wins_over_uniparental_disomy():
    assert _mode(4) == ("autosomal_dominant", ["uniparental_disomy"], False)


def test_mode_wins_over_unknown_parent_genotype():
    assert _mode(5) == ("autosomal_recessive", [], True)
    assert _origin(5) == "POSSIBLE_MOTHER"


def test_genotype_tag_without_mode():
    assert _mode(6) == ("unknown_proband_genotype", [], False)
    assert _origin(6) is None


def test_no_tag():
    assert _mode(7) == (None, [], False)
    assert _origin(7) is None


def test_family_id_in_a_tag_lists_no_sample():
    assert _mode(8) == (None, [], False)
    assert _origin(8) == "FATHER"


def test_tag_of_another_sample_only():
    assert _origin(9) is None


def test_process_occurrence_from_slivar():
    proband = _occurrences(SLIVAR_VCF, 2)[1]
    assert proband["parental_origin"] == "BOTH"
    assert proband["parental_origin_source"] == "slivar"
    assert proband["transmission_mode"] == "autosomal_recessive"
    assert proband["other_transmission_modes"] == ["autosomal_dominant"]
    assert proband["transmission_mode_candidate"] is False
    assert proband["transmission_mode_source"] == "slivar"


def test_process_occurrence_parents_get_no_inheritance():
    occurrences = _occurrences(SLIVAR_VCF, 2)
    for seq_id in (2, 3):
        for field in (
            "parental_origin",
            "transmission_mode",
            "other_transmission_modes",
            "transmission_mode_candidate",
            "parental_origin_source",
            "transmission_mode_source",
        ):
            assert occurrences[seq_id].get(field) is None


def test_process_occurrence_each_field_falls_back_on_its_own():
    proband = _occurrences(OLD_SLIVAR_VCF, 1, slivar=_methods(OLD_SLIVAR_VCF))[1]
    assert proband["parental_origin"] == "DENOVO"
    assert proband["parental_origin_source"] == "slivar"
    # Child 0/1, parents 0/0 and not affected: the ETL lookup gives a de novo.
    assert proband["transmission_mode"] == "autosomal_dominant_de_novo"
    assert proband["transmission_mode_source"] == "etl"
    assert proband.get("other_transmission_modes") is None
    assert proband.get("transmission_mode_candidate") is None


def test_process_occurrence_de_novo_tag_names_keep_the_etl_mode():
    proband = _occurrences(DE_NOVO_SLIVAR_VCF, 1, slivar=_methods(DE_NOVO_SLIVAR_VCF))[1]
    assert proband["parental_origin_source"] == "slivar"
    assert proband["transmission_mode"] == "autosomal_dominant_de_novo"
    assert proband["transmission_mode_source"] == "etl"


def test_process_occurrence_etl_method():
    proband = _occurrences(SLIVAR_VCF, 1, slivar=SlivarMethods())[1]
    assert proband["parental_origin"] == "DENOVO"
    assert proband["parental_origin_source"] == "etl"
    assert proband["transmission_mode"] == "autosomal_dominant_de_novo"
    assert proband["transmission_mode_source"] == "etl"
    assert proband.get("other_transmission_modes") is None


def test_process_occurrence_solo_keeps_empty_values():
    proband = _occurrences(SLIVAR_VCF, 1, ped=SOLO)[1]
    for field in ("parental_origin", "transmission_mode", "parental_origin_source", "transmission_mode_source"):
        assert proband.get(field) is None
