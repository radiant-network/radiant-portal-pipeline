"""COSMIC CMC denormalisation into `snv__staging_variant` (RAD-15).

Both staging statements, the ingest one (driven by `snv__tmp_variant`) and the re-annotation one (driven by
`snv__staging_variant` itself), must apply the same rule. The fields come from `cosmic_mutation_set` when
the locus is there. Otherwise all four come from `cosmic_mutation_set_hgvs`, matched on the picked
transcript and `c.` change. Otherwise they are NULL. The rule switches the whole row, never one column at
a time.
"""

import os

import jinja2
import pytest

from radiant.dags import DAGS_DIR

_SQL_DIR = os.path.join(DAGS_DIR, "sql")

_CMC_COLUMNS = ("cmc_mutation_url", "cmc_sample_mutated", "cmc_sample_ratio", "cmc_tier")
_COSMIC_COLUMNS = ("mutation_url", "sample_mutated", "sample_ratio", "tier")

# locus_id -> (transcript_id, dna_change) of the picked consequence.
_VARIANTS = {
    1: ("ENST00000000001", "c.1A>T"),  # in cosmic_mutation_set only
    2: ("ENST00000000002", "c.2A>T"),  # in cosmic_mutation_set_hgvs only
    3: ("ENST00000000003", "c.3A>T"),  # in both: the locus row wins, including its NULL tier
    4: ("ENST00000000004", "c.4A>T"),  # in neither
    5: ("ENST00000000005", "c.5A>T"),  # same c. change in cosmic_mutation_set_hgvs, but on another transcript
}

_COSMIC_BY_LOCUS = {
    1: ("https://cancer.sanger.ac.uk/cosmic/search?q=COSV1", 10, 0.1, "1"),
    3: ("https://cancer.sanger.ac.uk/cosmic/search?q=COSV3", 30, 0.3, None),
}

_COSMIC_BY_HGVS = {
    ("ENST00000000002", "c.2A>T"): ("https://cancer.sanger.ac.uk/cosmic/search?q=COSV2", 20, 0.2, "2"),
    ("ENST00000000003", "c.3A>T"): ("https://cancer.sanger.ac.uk/cosmic/search?q=COSV33", 99, 0.99, "Other"),
    ("ENST00000000099", "c.5A>T"): ("https://cancer.sanger.ac.uk/cosmic/search?q=COSV5", 50, 0.5, "3"),
}

_EXPECTED = {
    1: _COSMIC_BY_LOCUS[1],
    2: _COSMIC_BY_HGVS[("ENST00000000002", "c.2A>T")],
    3: _COSMIC_BY_LOCUS[3],
    4: (None, None, None, None),
    5: (None, None, None, None),
}

# Every relation the staging statements join, so they run against empty open-data tables.
_OPEN_DATA_TABLES = (
    "gnomad",
    "topmed_bravo",
    "1000_genomes",
    "clinvar",
    "dbsnp",
    "omim_gene_panel",
    "cosmic_mutation_set",
    "cosmic_mutation_set_hgvs",
)


def _render(path, mapping):
    with open(os.path.join(_SQL_DIR, path)) as f_in:
        return jinja2.Template(f_in.read()).render({"mapping": mapping})


def _seed(cursor, table, columns, rows):
    placeholders = ", ".join(["%s"] * len(columns))
    cursor.executemany(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})", rows)


@pytest.mark.parametrize(
    "statement",
    ["snv_staging_variant_insert.sql", "snv_staging_variant_reannotate.sql"],
)
def test_staging_variant_cmc_fields(starrocks_session, radiant_mapping, statement):
    staging = radiant_mapping["starrocks_snv_staging_variant"]

    with starrocks_session.cursor() as cursor:
        for table in _OPEN_DATA_TABLES:
            cursor.execute(_render(f"open_data/init/{table}_create_table.sql", radiant_mapping))
        for table in ("snv_tmp_variant", "snv_staging_variant"):
            cursor.execute(_render(f"radiant/init/{table}_create_table.sql", radiant_mapping))
        for key in (
            "starrocks_cosmic_mutation_set",
            "starrocks_cosmic_mutation_set_hgvs",
            "starrocks_snv_tmp_variant",
        ):
            cursor.execute(f"TRUNCATE TABLE {radiant_mapping[key]}")
        cursor.execute(f"TRUNCATE TABLE {staging}")

        _seed(
            cursor,
            radiant_mapping["starrocks_cosmic_mutation_set"],
            ("locus_id", *_COSMIC_COLUMNS),
            [(locus_id, *values) for locus_id, values in _COSMIC_BY_LOCUS.items()],
        )
        _seed(
            cursor,
            radiant_mapping["starrocks_cosmic_mutation_set_hgvs"],
            ("transcript_id", "cds_change", *_COSMIC_COLUMNS),
            [(*key, *values) for key, values in _COSMIC_BY_HGVS.items()],
        )

        variant_columns = ("locus_id", "chromosome", "start", "reference", "alternate", "transcript_id", "dna_change")
        variants = [(locus_id, "1", 1000 + locus_id, "A", "T", *pick) for locus_id, pick in _VARIANTS.items()]
        if statement == "snv_staging_variant_insert.sql":
            _seed(cursor, radiant_mapping["starrocks_snv_tmp_variant"], variant_columns, variants)
        else:
            # Stale values from an earlier COSMIC release: the re-annotation must replace them, NULLs included.
            _seed(
                cursor,
                staging,
                (*variant_columns, *_CMC_COLUMNS),
                [(*v, "https://stale", 1, 0.01, "Other") for v in variants],
            )

        cursor.execute(_render(f"radiant/{statement}", radiant_mapping))
        cursor.execute(f"SELECT locus_id, {', '.join(_CMC_COLUMNS)} FROM {staging} ORDER BY locus_id")
        actual = {row[0]: tuple(row[1:]) for row in cursor.fetchall()}

    assert actual == _EXPECTED
