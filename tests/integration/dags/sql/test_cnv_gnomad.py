"""RAD-194 -- the gnomAD SV columns of the CNV occurrences, `gnomad_ac` in particular.

The ingest (`*_cnv_occurrence_insert_partition_delta.sql`) and the re-annotation
(`*_cnv_occurrence_reannotate_partition.sql`, which does the backfill) both match each CNV to one gnomAD SV call
(`gnomad_sv_v1`, fixture `tests/resources/integration/open_data/gnomad_sv_v1.tsv`) and copy its AF, AC, SC and
SF. A CNV that matches nothing must keep NULL in all four, not 0: 0 would read as "seen in gnomAD, never carried".
"""

import os

import jinja2
import pyarrow as pa
import pytest
from pyiceberg.expressions import EqualTo

from tests.integration.dags.sql.test_cnv_nb_snv import _SQL_DIR, _create_cnv_id_udf, _create_table, _insert_rows

# Own part and task_id, apart from test_cnv_nb_snv.py's, so the rows are addressable for the teardown.
_PART = 1940
_TASK_ID = 19400
_TENANT = "test"
_SEQ_ID = 1940001

# gnomad_sv_v1.tsv, row `1 10000000 10500000 DUP`: the only call on chromosome 1 that a GAIN over the same
# interval overlaps reciprocally by 80%.
_GNOMAD_START = 10_000_000
_GNOMAD_END = 10_500_000
_GNOMAD_AF = 0.0025
_GNOMAD_AC = 54
_GNOMAD_SC = 54  # n_het + n_homalt
_GNOMAD_SN = 21694  # n_bi_genos

_MATCH = "cnv-gnomad-match"
# Same interval as gnomad_sv_v1's chromosome 2 call, but that call is a DEL: the type mapping keeps it out.
_WRONG_TYPE = "cnv-gnomad-wrong-type"
# No gnomAD call on chromosome 3 at all.
_NO_CALL = "cnv-gnomad-no-call"

_GNOMAD_COLUMNS = ("gnomad_af", "gnomad_ac", "gnomad_sc", "gnomad_sf")


def _cnv_row(*, name, chromosome, start, end, cnv_type, alternate):
    return {
        "part": _PART,
        "seq_id": _SEQ_ID,
        "tenant_code": _TENANT,
        "task_id": _TASK_ID,
        "aliquot": f"SA{_SEQ_ID}",
        "chromosome": chromosome,
        "alternate": alternate,
        "start": start,
        "end": end,
        "type": cnv_type,
        "length": end - start,
        "name": name,
    }


_CNV_ROWS = [
    _cnv_row(name=_MATCH, chromosome="1", start=_GNOMAD_START, end=_GNOMAD_END, cnv_type="GAIN", alternate="<DUP>"),
    _cnv_row(name=_WRONG_TYPE, chromosome="2", start=20_000_000, end=21_000_000, cnv_type="GAIN", alternate="<DUP>"),
    _cnv_row(name=_NO_CALL, chromosome="3", start=1_000_000, end=2_000_000, cnv_type="LOSS", alternate="<DEL>"),
]


def _render(file_name, mapping):
    with open(os.path.join(_SQL_DIR, "radiant", file_name)) as f_in:
        return jinja2.Template(f_in.read()).render({"mapping": mapping, "partition": _PART})


def _create_tables(starrocks_session, mapping, flavour):
    # Only created, never truncated: the statements join them, but nothing here asserts what they feed, and
    # other tests of the session own their content.
    for table in (f"{flavour}_cnv_occurrence", f"{flavour}_snv_occurrence", "snv_variant"):
        _create_table(starrocks_session, "radiant", table, mapping, truncate=False)
    for table in ("cytoband", "ensembl_gene"):
        _create_table(starrocks_session, "open_data", table, mapping, truncate=False)
    _create_cnv_id_udf(starrocks_session, mapping)


def _gnomad_by_name(starrocks_session, mapping, flavour):
    with starrocks_session.cursor() as cursor:
        cursor.execute(
            f"SELECT name, {', '.join(_GNOMAD_COLUMNS)} "
            f"FROM {mapping[f'starrocks_{flavour}_cnv_occurrence']} WHERE part = %(part)s",
            {"part": _PART},
        )
        return {row[0]: dict(zip(_GNOMAD_COLUMNS, row[1:], strict=True)) for row in cursor.fetchall()}


def _assert_gnomad(gnomad):
    assert set(gnomad) == {_MATCH, _WRONG_TYPE, _NO_CALL}

    match = gnomad[_MATCH]
    assert match["gnomad_ac"] == _GNOMAD_AC
    assert match["gnomad_af"] == pytest.approx(_GNOMAD_AF)
    assert match["gnomad_sc"] == _GNOMAD_SC
    assert match["gnomad_sf"] == pytest.approx(_GNOMAD_SC / _GNOMAD_SN)

    for name in (_WRONG_TYPE, _NO_CALL):
        assert gnomad[name] == dict.fromkeys(_GNOMAD_COLUMNS), f"{name}: no gnomAD SV call matches, expected NULL"


@pytest.fixture(params=["germline", "somatic"])
def flavour(
    request,
    starrocks_session,
    iceberg_client,
    setup_iceberg_namespace,
    # For `gnomad_sv_v1`, which the CNV statements LEFT JOIN and which exists only in Iceberg.
    open_data_iceberg_tables,
    radiant_mapping,
):
    flavour = request.param
    _create_tables(starrocks_session, radiant_mapping, flavour)
    target = radiant_mapping[f"starrocks_{flavour}_cnv_occurrence"]
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"DELETE FROM {target} WHERE part = %(part)s", {"part": _PART})

    yield flavour

    with starrocks_session.cursor() as cursor:
        cursor.execute(f"DELETE FROM {target} WHERE part = %(part)s", {"part": _PART})


@pytest.fixture
def seeded_iceberg(flavour, starrocks_session, iceberg_client, setup_iceberg_namespace, radiant_mapping):
    iceberg_table = iceberg_client.load_table(f"{setup_iceberg_namespace}.{flavour}_cnv_occurrence")
    # The table's own schema: Iceberg renumbers field ids on create, and the append is validated by id.
    iceberg_table.append(pa.Table.from_pylist(_CNV_ROWS, schema=iceberg_table.schema().as_arrow()))
    # StarRocks caches Iceberg metadata; without the refresh the insert reads a pre-append snapshot.
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"REFRESH EXTERNAL TABLE {radiant_mapping[f'iceberg_{flavour}_cnv_occurrence']}")
    yield flavour
    # The Iceberg CNV tables are shared with the VCF tests, which assert unfiltered row counts on them.
    iceberg_table.delete(EqualTo("task_id", _TASK_ID))


def test_ingest_stores_the_gnomad_sv_ac(seeded_iceberg, starrocks_session, radiant_mapping):
    """AC 1 (germline), AC 2 (somatic), AC 3 (no match: NULL, not 0)."""
    flavour = seeded_iceberg
    target = radiant_mapping[f"starrocks_{flavour}_cnv_occurrence"]
    sql = _render(f"{flavour}_cnv_occurrence_insert_partition_delta.sql", radiant_mapping)
    with starrocks_session.cursor() as cursor:
        # Positional, the way `RadiantStarRocksPartitionSwapOperator` runs it: fails if the column order drifts
        # from the create statement.
        cursor.execute(
            f"INSERT INTO {target} {sql.rstrip().rstrip(';')}",
            # `task_ids` as `import_part` passes it: the germline insert filters on the run's tasks (RAD-169).
            {"seq_ids": [_SEQ_ID], "task_ids": [_TASK_ID], "tenant_code": _TENANT},
        )

    _assert_gnomad(_gnomad_by_name(starrocks_session, radiant_mapping, flavour))


def test_reannotation_backfills_the_gnomad_sv_ac(flavour, starrocks_session, radiant_mapping):
    """AC 4: the re-annotation fills `gnomad_ac` on CNVs loaded without it, and clears stale values.

    The rows are seeded straight into StarRocks, as a part imported before the migration looks: the matching
    CNV has a NULL AC, the others carry stale values that must not survive.
    """
    target = radiant_mapping[f"starrocks_{flavour}_cnv_occurrence"]
    stale = (0.5, 999, 999, 0.5)
    columns = ("part", "seq_id", "task_id", "cnv_id", "chromosome", "start", "end", "type", "length", "name")
    rows = [
        (*(r[c] for c in ("part", "seq_id", "task_id")), cnv_id, *(r[c] for c in columns[4:]), *gnomad)
        for cnv_id, r, gnomad in (
            (1, _CNV_ROWS[0], (None, None, None, None)),
            (2, _CNV_ROWS[1], stale),
            (3, _CNV_ROWS[2], stale),
        )
    ]
    _insert_rows(starrocks_session, target, (*columns, *_GNOMAD_COLUMNS), rows)

    sql = _render(f"{flavour}_cnv_occurrence_reannotate_partition.sql", radiant_mapping)
    with starrocks_session.cursor() as cursor:
        cursor.execute(sql.rstrip().rstrip(";"), {"part": _PART})

    _assert_gnomad(_gnomad_by_name(starrocks_session, radiant_mapping, flavour))
