import logging
from collections import Counter

import pytest

from radiant.tasks.vcf.experiment import Experiment, RadiantGermlineAnnotationTask
from radiant.tasks.vcf.snv.common import Common
from radiant.tasks.vcf.snv.consequence import (
    log_source_counts,
    parse_csq_header,
    process_consequence,
    resolve_source,
    strip_transcript_version,
)
from tests.unit.vcf.vcf_test_utils import variant, vcf

task = RadiantGermlineAnnotationTask(
    task_id=1,
    part=1,
    analysis_type="germline",
    deleted=False,
    experiments=[
        Experiment(
            seq_id=1,
            patient_id=1,
            aliquot="SA0001",
            tenant_code="tenant1",
            family_role="proband",
            affected_status="affected",
            sex="F",
            experimental_strategy="wgs",
            request_priority="routine",
        )
    ],
    vcf_filepath="",
)
common = Common(task.task_id, task.part, "1-1000-AC-A", "hash", "1", 1000, 1000, "AC", "A")


def test_one_sample():
    v = variant("test_consequence_one_sample.vcf")
    with vcf("test_consequence_one_sample.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    expected_picked = {
        "aa_change": "p.Lys76Asn",
        "alternate": "A",
        "biotype": "protein_coding",
        "task_id": 1,
        "chromosome": "1",
        "consequences": ["missense_variant"],
        "dna_change": "c.227A>T",
        "end": 1000,
        "exon": {"rank": "2-3", "total": "8"},
        "hgvsc": "c.227A>T",
        "hgvsg": "g.12345G>A",
        "hgvsp": "p.Lys76Asn",
        "impact_score": 3,
        "is_canonical": True,
        "is_mane_plus": False,
        "is_mane_select": False,
        "is_picked": True,
        "locus": "1-1000-AC-A",
        "locus_hash": "hash",
        "mane_select": "ENST00000357654.7",
        "mane_pair_transcript_id": "ENST00000357654",
        "reference": "AC",
        "source": "Ensembl",
        "start": 1000,
        "strand": "1",
        "symbol": "BRCA1",
        "transcript_id": "ENST00000357654",
        "transcript_id_unversioned": "ENST00000357654",
        "variant_class": "SNV",
        "vep_impact": "MODERATE",
    }

    picked, consequences = process_consequence(v, csq_header, common)
    assert picked == expected_picked
    assert consequences is not None


def test_one_sample_second_consequence_is_refseq():
    v = variant("test_consequence_one_sample.vcf", row_number=2)
    with vcf("test_consequence_one_sample.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    _, consequences = process_consequence(v, csq_header, common)

    assert len(consequences) == 1
    assert consequences[0]["source"] == "RefSeq"
    assert consequences[0]["transcript_id"] == "NM_000546.5"
    # On a RefSeq row the MANE cross-reference points at the Ensembl twin.
    assert consequences[0]["mane_select"] == "ENST00000269305.9"


def test_source_absent_from_header_resolves_from_identifiers():
    # test_somatic_snv.vcf declares MANE_SELECT but no SOURCE, so the source has to come from
    # the identifiers instead — its blocks all carry ENST/ENSG. The mane_select assertion is
    # the absent-vs-empty coverage: an absent column stays null, a declared-but-empty one is "".
    v = variant("test_somatic_snv.vcf")
    with vcf("test_somatic_snv.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    assert "source" not in csq_header

    _, consequences = process_consequence(v, csq_header, common)

    assert consequences
    assert all(c["source"] == "Ensembl" for c in consequences)
    assert all(c["mane_select"] == "" for c in consequences)


def test_a_non_merged_file_extracts_exactly_as_before():
    # Regression guard for the files we already ingest. `test_somatic_snv.vcf` predates merged
    # support: it has no SOURCE column, so the source has to come from the identifiers, and
    # every other field must be untouched by the merged-file work. Asserted as a whole dict so
    # a field changing silently cannot slip through.
    v = variant("test_somatic_snv.vcf", row_number=4)
    with vcf("test_somatic_snv.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    assert "source" not in csq_header

    picked, consequences = process_consequence(v, csq_header, common)

    assert len(consequences) == 3
    assert picked == {
        "aa_change": None,
        "alternate": "A",
        "biotype": "protein_coding",
        "chromosome": "1",
        "consequences": ["upstream_gene_variant"],
        "dna_change": None,
        "end": 1000,
        "exon": None,
        "hgvsc": "",
        "hgvsg": "chr1:g.64976C>T",
        "hgvsp": "",
        "impact_score": 1,
        "is_canonical": True,
        # The `MANE` column exists here but is empty, while `MANE_SELECT` is populated. That is
        # what makes this row worth pinning: the flag stays False and the pair is still derived,
        # which is the independence the merged work relies on.
        "is_mane_plus": False,
        "is_mane_select": False,
        "is_picked": True,
        "locus": "1-1000-AC-A",
        "locus_hash": "hash",
        "mane_pair_transcript_id": "NM_001005484",
        "mane_select": "NM_001005484.2",
        "reference": "AC",
        "source": "Ensembl",
        "start": 1000,
        "strand": "1",
        "symbol": "OR4F5",
        "task_id": 1,
        "transcript_id": "ENST00000641515",
        "transcript_id_unversioned": "ENST00000641515",
        "variant_class": "SNV",
        "vep_impact": "MODIFIER",
    }


def test_source_column_takes_precedence_over_identifiers():
    # Block 1 declares RefSeq while carrying an ENST transcript and an ENSG gene: VEP's own
    # answer wins. Block 2's SOURCE is declared but empty, and block 3's is a value we do not
    # recognise — both fall through to the identifiers rather than short-circuiting to null.
    v = variant("test_consequence_source_precedence.vcf")
    with vcf("test_consequence_source_precedence.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    _, consequences = process_consequence(v, csq_header, common)

    assert [c["source"] for c in consequences] == ["RefSeq", "RefSeq", "Ensembl"]


def test_source_falls_back_to_feature_then_gene_identifiers():
    # No SOURCE column at all — the non-merged file case. One block per rung of the fallback:
    # ENST / LRG / ENSR (regulatory, and the only one with no gene) resolve on the transcript,
    # NM / XR / NP resolve on the accession, the two feature-less blocks resolve on the gene,
    # and the intergenic block has nothing to resolve on.
    v = variant("test_consequence_source_fallback.vcf")
    with vcf("test_consequence_source_fallback.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    _, consequences = process_consequence(v, csq_header, common)

    assert [c["source"] for c in consequences] == [
        "Ensembl",
        "Ensembl",
        "Ensembl",
        "RefSeq",
        "RefSeq",
        "RefSeq",
        "Ensembl",
        "RefSeq",
        None,
    ]


def test_intergenic_row_is_kept_with_no_source():
    # A block with neither a gene nor a transcript belongs to no catalogue. It must be
    # reported as unclassified, not dropped and not forced into one of the two sources.
    v = variant("test_consequence_source_fallback.vcf")
    with vcf("test_consequence_source_fallback.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    _, consequences = process_consequence(v, csq_header, common)

    intergenic = consequences[-1]
    assert intergenic["source"] is None
    assert intergenic["consequences"] == ["intergenic_variant"]


def test_csq_field_names_are_matched_case_insensitively():
    # VEP spells these columns differently across versions and plugins, so the lookup
    # must not depend on the casing used in the header.
    v = variant("test_consequence_lowercase_csq_no_exon.vcf")
    with vcf("test_consequence_lowercase_csq_no_exon.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    picked, _ = process_consequence(v, csq_header, common)

    assert picked["source"] == "Ensembl"
    assert picked["mane_select"] == "NM_007294.3"


def test_missing_exon_field_does_not_raise():
    # EXON is optional: `.split("/")` on the missing value used to raise AttributeError.
    v = variant("test_consequence_lowercase_csq_no_exon.vcf")
    with vcf("test_consequence_lowercase_csq_no_exon.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    picked, _ = process_consequence(v, csq_header, common)

    assert picked["exon"] is None
    assert picked["symbol"] == "BRCA1"


def mane_consequences():
    v = variant("test_consequence_mane.vcf")
    with vcf("test_consequence_mane.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)
    _, consequences = process_consequence(v, csq_header, common)
    return consequences


def test_mane_flags_are_derived_from_the_mane_column():
    # The flags come from `MANE`, not from `MANE_SELECT` — the two are different columns. The
    # fixture's blocks are, in order: an Ensembl MANE Select transcript, its RefSeq twin, a
    # MANE Plus Clinical transcript, and a transcript that is neither.
    consequences = mane_consequences()

    assert [(c["is_mane_select"], c["is_mane_plus"]) for c in consequences] == [
        (True, False),
        (True, False),
        (False, True),
        (False, False),
    ]


def test_mane_flags_are_absent_when_the_mane_column_is():
    # A file with no `MANE` column cannot claim either flag. Both must still be booleans, since
    # the schema declares them required.
    v = variant("test_consequence_one_sample.vcf")
    with vcf("test_consequence_one_sample.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    assert "mane" not in csq_header

    picked, _ = process_consequence(v, csq_header, common)

    assert picked["is_mane_select"] is False
    assert picked["is_mane_plus"] is False


def test_mane_pair_transcript_id_is_unversioned():
    # `MANE_SELECT` always arrives versioned; the stored pair never is. VEP leaves the column
    # empty on MANE Plus Clinical rows, so those carry no pair to strip.
    consequences = mane_consequences()

    assert [c["mane_select"] for c in consequences] == ["NM_000546.6", "ENST00000269305.9", "", ""]
    assert [c["mane_pair_transcript_id"] for c in consequences] == ["NM_000546", "ENST00000269305", "", ""]


def test_mane_pair_joins_the_twins_transcript_id_in_both_directions():
    # The acceptance criterion. Both sides are stripped because RefSeq `Feature` values are
    # versioned while Ensembl ones are not: stripping only the cross-reference would leave the
    # Ensembl→RefSeq direction matching nothing, silently.
    ensembl, refseq = mane_consequences()[:2]

    assert ensembl["mane_pair_transcript_id"] == refseq["transcript_id_unversioned"] == "NM_000546"
    assert refseq["mane_pair_transcript_id"] == ensembl["transcript_id_unversioned"] == "ENST00000269305"


def test_refseq_transcript_id_keeps_its_version():
    # Only the derived column is stripped. The versioned accession stays available for display,
    # which is the form a clinician cites.
    refseq = mane_consequences()[1]

    assert refseq["source"] == "RefSeq"
    assert refseq["transcript_id"] == "NM_000546.6"
    assert refseq["transcript_id_unversioned"] == "NM_000546"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        # An absent CSQ column reads as None and a declared-but-empty one as "". Both are
        # preserved as is, because callers distinguish them.
        (None, None),
        ("", ""),
        # Ensembl `Feature` values arrive unversioned already, so stripping is a no-op...
        ("ENST00000269305", "ENST00000269305"),
        ("LRG_214t1", "LRG_214t1"),
        # ...while RefSeq `Feature` values and every `MANE_SELECT` value are versioned.
        ("ENST00000269305.9", "ENST00000269305"),
        ("NM_000546.6", "NM_000546"),
        ("XR_935622.3", "XR_935622"),
        # Only the first `.` matters, whatever follows it.
        ("NM_000546.6.1", "NM_000546"),
    ],
)
def test_strip_transcript_version(value, expected):
    assert strip_transcript_version(value) == expected


@pytest.mark.parametrize(
    ("source", "transcript_id", "gene_id", "expected"),
    [
        # Rule 1 — VEP states the source itself and is believed over the identifiers.
        ("Ensembl", "NM_000546.5", "7157", "Ensembl"),
        ("RefSeq", "ENST00000269305", "ENSG00000141510", "RefSeq"),
        # ... whatever case it is spelled in, since VEP is not consistent across versions.
        ("ensembl", None, None, "Ensembl"),
        ("REFSEQ", None, None, "RefSeq"),
        ("  RefSeq  ", None, None, "RefSeq"),
        # An absent column reads as None and a declared-but-empty one as "". Neither says
        # anything about the catalogue, so both fall through to the identifiers.
        (None, "ENST00000357654", None, "Ensembl"),
        ("", "NM_000546.5", None, "RefSeq"),
        # An unexpected value falls through too, keeping the column a closed set.
        ("Unknown", "ENST00000357654", None, "Ensembl"),
        ("Unknown", None, None, None),
        # Rule 2 — the transcript namespaces are disjoint between the two catalogues.
        (None, "ENST00000357654", "", "Ensembl"),
        (None, "LRG_214t1", "", "Ensembl"),
        (None, "ENSR00000105975", "", "Ensembl"),  # regulatory features carry no gene at all
        (None, "NM_000546.5", "", "RefSeq"),
        (None, "NR_003051.3", "", "RefSeq"),
        (None, "NP_000537.3", "", "RefSeq"),
        (None, "XM_005260958.1", "", "RefSeq"),
        (None, "XR_935622.3", "", "RefSeq"),
        (None, "XP_005261015.1", "", "RefSeq"),
        # Rule 3 — the only signal left when the block carries no feature id.
        (None, "", "ENSG00000141510", "Ensembl"),
        (None, "", "7157", "RefSeq"),
        # Matching is anchored: a gene symbol that merely contains a namespace prefix, or an
        # identifier that merely starts with one, must not be classified.
        (None, "", "BRCA1", None),
        (None, "", "ENSA", None),
        (None, "", "ENSG00000141510.4", None),
        (None, "TENST00000357654", "", None),
        (None, "ENSM00000105975", "", None),
        # Rule 4 — intergenic blocks belong to no catalogue and are not forced into one.
        (None, None, None, None),
        ("", "", "", None),
    ],
)
def test_resolve_source_rules(source, transcript_id, gene_id, expected):
    assert resolve_source(source, transcript_id, gene_id) == expected


def test_log_source_counts_reports_the_breakdown(caplog):
    caplog.set_level(logging.INFO, logger="airflow.task")

    log_source_counts(Counter({"Ensembl": 3, "RefSeq": 2, None: 1}), task_id=1, vcf_filepath="/tmp/merged.vcf.gz")

    assert "Ensembl=3" in caplog.text
    assert "RefSeq=2" in caplog.text
    assert "unclassified=1" in caplog.text


def test_log_source_counts_warns_on_unclassified_rows(caplog):
    caplog.set_level(logging.INFO, logger="airflow.task")

    log_source_counts(Counter({"Ensembl": 3, None: 2}), task_id=1, vcf_filepath="/tmp/merged.vcf.gz")

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "2 consequence rows" in warnings[0].message
    assert "/tmp/merged.vcf.gz" in warnings[0].message


def test_log_source_counts_is_quiet_when_every_row_is_classified(caplog):
    caplog.set_level(logging.INFO, logger="airflow.task")

    log_source_counts(Counter({"Ensembl": 3, "RefSeq": 2}), task_id=1, vcf_filepath="/tmp/merged.vcf.gz")

    assert not [r for r in caplog.records if r.levelno == logging.WARNING]
    assert "unclassified=0" in caplog.text
