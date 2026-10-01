"""End-to-end check of the COSMIC Mutation Census StarRocks steps: broker load of the normalized TSV into the
staging table (keyed and keyless rows alike, alleles longer than 2,000 bases included), the variant_lookup
registration, the per-locus deduplication into cosmic_mutation_set, and the per-(transcript, c.) build of
cosmic_mutation_set_hgvs from the keyless rows.

The normalization itself (anchor bases + bcftools) is covered by tests/unit/open_data; this starts from the
file it writes.
"""

import os
import time
import uuid

import jinja2

from radiant.dags import DAGS_DIR

_SQL_DIR = os.path.join(DAGS_DIR, "sql")


def _wait_for_load(cursor, label):
    for _ in range(150):
        cursor.execute(
            "SELECT STATE, ERROR_MSG FROM information_schema.loads WHERE LABEL = %(label)s", {"label": label}
        )
        row = cursor.fetchone()
        if not row or row[0] == "FINISHED":
            return
        if row[0] == "CANCELLED":
            raise RuntimeError(f"Load {label} was cancelled: {row[1]}")
        time.sleep(2)
    raise TimeoutError(f"Load for label {label} did not finish in time.")


def test_cosmic_mutation_set_load_and_dedup(
    starrocks_session, radiant_mapping, minio_instance, sample_cosmic_mutation_set_normalized_tsv
):
    raw = radiant_mapping["starrocks_raw_cosmic_mutation_set"]
    final = radiant_mapping["starrocks_cosmic_mutation_set"]
    hgvs = radiant_mapping["starrocks_cosmic_mutation_set_hgvs"]
    lookup = radiant_mapping["starrocks_variant_lookup"]
    database_name, table_name = raw.split(".")
    label = f"test_cosmic_mutation_set_load_{uuid.uuid4().hex}"

    def render(path, **context):
        with open(os.path.join(_SQL_DIR, path)) as f_in:
            return jinja2.Template(f_in.read()).render({"mapping": radiant_mapping, **context})

    load_sql = render(
        "open_data/cosmic_mutation_set_load.sql",
        broker_load_timeout=7200,
        database_name=database_name,
        table_name=table_name,
        load_label=label,
        broker_configuration=f"""
            'aws.s3.region' = 'us-east-1',
            'aws.s3.endpoint' = 'http://radiant-minio:9000',
            'aws.s3.enable_path_style_access' = 'true',
            'aws.s3.access_key' = '{minio_instance.access_key}',
            'aws.s3.secret_key' = '{minio_instance.secret_key}'
        """,
    )

    with starrocks_session.cursor() as cursor:
        cursor.execute(render("radiant/init/variant_id_udf.sql", params={"udf_release_version": "v2.0.0"}))
        cursor.execute(render("radiant/init/variant_lookup_create_table.sql"))
        cursor.execute(render("open_data/init/raw_cosmic_mutation_set_create_table.sql"))
        cursor.execute(render("open_data/init/cosmic_mutation_set_create_table.sql"))
        cursor.execute(render("open_data/init/cosmic_mutation_set_hgvs_create_table.sql"))
        cursor.execute(f"TRUNCATE TABLE {raw}")

        # The DAG passes a one-element list; pymysql renders it as `('s3://...')`, which DATA INFILE requires.
        cursor.execute(load_sql, {"tsv_filepath": [f"s3://{sample_cosmic_mutation_set_normalized_tsv}"]})
        cursor.fetchall()
        _wait_for_load(cursor, label)

        cursor.execute(
            f"SELECT cosmic_id, chromosome, start, reference, alternate, shared_aa, tier, locus_hash, transcript_id, "
            f"cds_change FROM {raw}"
        )
        staged = {r[0]: r[1:] for r in cursor.fetchall()}
        assert set(staged) == {
            "COSV61373102",
            "COSV99999999",
            "COSV60102180",
            "COSV56056643",
            "COSV115976235",
            "COSV105246280",
            "COSV88888888",
            "COSV88888889",
        }
        assert staged["COSV61373102"][:6] == ("1", 26731445, "G", "GC", 5, "3")
        assert staged["COSV61373102"][7:] == ("ENST00000324856", "c.1650dup"), "accession without its version"
        assert staged["COSV60102180"][:6] == ("20", 32434638, "AG", "A", 3, "3")
        assert staged["COSV56056643"][4:6] == (None, "1"), "an empty SHARED_AA cell is NULL"
        # The QA failure: a 2,501-base reference allele must load, not cancel the job.
        assert len(staged["COSV105246280"][2]) == 2501
        # Rows the normalizer could not place arrive with their five key cells empty and are stored NULL.
        for keyless in ("COSV88888888", "COSV88888889"):
            assert staged[keyless][:4] == (None, None, None, None) and staged[keyless][6] is None
            assert staged[keyless][7:] == ("ENST00000324856", "c.1651dup")

        cursor.execute(render("open_data/cosmic_mutation_set_insert_hashes.sql"))
        cursor.execute(render("open_data/cosmic_mutation_set_insert.sql"))

        cursor.execute(
            f"SELECT cosmic_id, locus_id, sample_mutated, sample_tested, sample_ratio, tier, shared_aa FROM {final}"
        )
        rows = {r[0]: r[1:] for r in cursor.fetchall()}
        # One row per locus: the second transcript row on 1-26731445-G-GC lost to the higher sample_mutated,
        # and the keyless rows are not here at all.
        assert set(rows) == {"COSV61373102", "COSV60102180", "COSV56056643", "COSV115976235", "COSV105246280"}
        assert all(r[0] is not None for r in rows.values()), "every row must resolve a locus_id"
        assert rows["COSV61373102"][1:] == (24, 100149, 24 / 100149, "3", 5)
        assert rows["COSV60102180"][1:3] == (87, 104260)
        assert rows["COSV56056643"][5] is None

        # The long deletion has no packed id: it must have been registered in variant_lookup and resolved there.
        cursor.execute(
            f"SELECT r.locus_id, v.locus_id FROM {raw} t "
            f"JOIN {final} r ON r.cosmic_id = t.cosmic_id "
            f"LEFT JOIN {lookup} v ON v.locus_hash = t.locus_hash "
            f"WHERE t.cosmic_id = 'COSV115976235'"
        )
        resolved, looked_up = cursor.fetchone()
        cursor.execute(
            "SELECT GET_VARIANT_ID('X', 18650529, %(ref)s, 'T')", {"ref": "T" + "TCCATGTGCCCGACACTCCAGGTCCGAGGCACT"}
        )
        (packed,) = cursor.fetchone()
        if packed is None:
            assert looked_up is not None and resolved == looked_up
        else:
            assert resolved == packed

        # The HGVS table: exactly the keyless rows, one per (transcript, c.), highest sample_mutated first.
        cursor.execute(render("open_data/cosmic_mutation_set_hgvs_insert.sql"))
        cursor.execute(
            f"SELECT transcript_id, cds_change, cosmic_id, sample_mutated, sample_tested, sample_ratio, tier, "
            f"shared_aa FROM {hgvs}"
        )
        hgvs_rows = cursor.fetchall()
        assert hgvs_rows == (("ENST00000324856", "c.1651dup", "COSV88888888", 9, 50000, 9 / 50000, "2", 4),)
        # Never a mutation that cosmic_mutation_set already holds.
        cursor.execute(f"SELECT count(*) FROM {hgvs} h JOIN {final} c ON c.cosmic_id = h.cosmic_id")
        assert cursor.fetchone() == (0,)
