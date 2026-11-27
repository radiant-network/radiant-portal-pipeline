"""ACMG classification of the affected probands of a tenant, or of some of them, in three steps.

1. variant:        variant facts -> criteria met by each variant              -> acmg_variant_criteria
2. occurrence:     occurrence facts -> criteria met by the case's own evidence -> acmg_occurrence_criteria
3. classification: both criteria tables, per occurrence -> class                -> acmg_classification

Each step: StarRocks unloads its facts to Parquet on S3 (`INSERT INTO FILES()`), N workers apply the rules in
Polars, StarRocks loads their files back (`INSERT ... SELECT FROM FILES()`) after deleting the rows of the scope,
so that a rerun (new ClinVar, new occurrences) replaces them. Prints counts and timings only.

S3 settings come from the environment (defaults: the local RustFS):
S3_ENDPOINT, S3_ENDPOINT_STARROCKS (as the BEs reach it), S3_REGION, S3_BUCKET (bucket or bucket/prefix), and either
S3_ACCESS_KEY + S3_SECRET_KEY or S3_USE_INSTANCE_PROFILE=true (the pod's role for the workers, the BEs' role for
StarRocks).
"""

import argparse
import json
import os
import resource
import sys
import time
import uuid
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from string import Template
from urllib.parse import urlparse

import polars as pl
import pyarrow.parquet as pq
from acmg import citations, clingen, cspec, erepo, hgnc, hpo, protein
from acmg.db import HERE, IDENTIFIERS, connect, execute, sql
from acmg.models import OCCURRENCE_FACT_SCHEMA, VARIANT_FACT_SCHEMA, Ruleset
from acmg.rules import OCCURRENCE_CRITERIA, VARIANT_CRITERIA, combine, evaluate, to_wide
from pyarrow import fs

CRITERION_COLUMNS = [
    "criterion", "code", "strength", "points", "value", "threshold", "source", "details", "refs", "ruleset_version"
]
CLASS_COLUMNS = ["acmg_class", "acmg_points", "acmg_criteria", "acmg_cancelled", "ruleset_version"]
PHENOTYPE_COLUMNS = ["seq_id", "symbol", "n_terms", "score", "top", "pp4_points", "ruleset_version"]
LEVELS = {
    "variant": {
        "facts": "variant_facts.sql",
        "table": "acmg_variant_criteria",
        "keys": ["locus_id", "transcript_id"],
        "columns": CRITERION_COLUMNS,
        "scope": "locus_id IN (SELECT locus_id FROM (${classified}) k)",
    },
    "occurrence": {
        "facts": "occurrence_facts.sql",
        "table": "acmg_occurrence_criteria",
        "keys": ["seq_id", "locus_id"],
        "columns": CRITERION_COLUMNS,
        "scope": "seq_id IN (${cases})",
    },
    "classification": {
        "facts": "classification_facts.sql",
        "table": "acmg_classification",
        "keys": ["seq_id", "locus_id", "transcript_id"],
        "columns": CLASS_COLUMNS,
        "scope": "seq_id IN (${cases})",
    },
}


def s3_settings() -> dict:
    instance_profile = os.environ.get("S3_USE_INSTANCE_PROFILE", "").lower() == "true"
    return {
        "endpoint": os.environ.get("S3_ENDPOINT", "" if instance_profile else "http://localhost:19000"),
        "endpoint_starrocks": os.environ.get("S3_ENDPOINT_STARROCKS", "" if instance_profile else "http://rustfs:9000"),
        "instance_profile": instance_profile,
        "access_key": os.environ.get("S3_ACCESS_KEY", "" if instance_profile else "admin"),
        "secret_key": os.environ.get("S3_SECRET_KEY", "" if instance_profile else "password"),
        "region": os.environ.get("S3_REGION", "us-east-1"),
        "bucket": os.environ.get("S3_BUCKET", "acmg-poc"),
    }


def filesystem() -> fs.S3FileSystem:
    s = s3_settings()
    options = {"region": s["region"], "allow_bucket_creation": not s["instance_profile"]}
    if s["endpoint"]:
        endpoint = urlparse(s["endpoint"])
        options |= {"endpoint_override": endpoint.netloc, "scheme": endpoint.scheme}
    if not s["instance_profile"]:
        options |= {"access_key": s["access_key"], "secret_key": s["secret_key"]}
    return fs.S3FileSystem(**options)


def files_properties(path: str) -> str:
    """FILES() properties for StarRocks; secrets stay in the environment, never in a committed file."""
    s = s3_settings()
    props = {"path": path, "format": "parquet", "aws.s3.region": s["region"]}
    if s["endpoint_starrocks"]:
        props |= {"aws.s3.endpoint": s["endpoint_starrocks"], "aws.s3.enable_path_style_access": "true"}
    if s["instance_profile"]:
        props["aws.s3.use_instance_profile"] = "true"
    else:
        props |= {"aws.s3.access_key": s["access_key"], "aws.s3.secret_key": s["secret_key"]}
    return ",\n".join(f'"{k}" = "{v}"' for k, v in props.items())


def scope(args, rs: Ruleset) -> dict:
    """Template parameters of the three facts queries: the cases asked for and the (locus, transcript) classified."""
    for key in ("shared_db", "tenant_db", "output_db", "tenant_code", "clinical_db"):
        if not IDENTIFIERS[key].fullmatch(getattr(args, key)):
            raise ValueError(f"invalid {key}: {getattr(args, key)!r}")
    part = "" if args.part is None else f"AND o.part = {int(args.part)}"
    if args.seq_ids:
        seq_ids = f"AND se.seq_id IN ({', '.join(str(int(s)) for s in args.seq_ids)})"
    elif args.part is not None:
        occurrences = f"{args.tenant_db}.germline__snv__occurrence"
        seq_ids = f"AND se.seq_id IN (SELECT DISTINCT p.seq_id FROM {occurrences} p WHERE p.part = {int(args.part)})"
    else:
        seq_ids = ""
    # Qualified columns: the fragment also sits in DELETE subqueries.
    cases = (
        f"SELECT se.seq_id FROM {args.shared_db}.staging_sequencing_experiment se"
        f" WHERE se.analysis_type = 'germline' AND se.tenant_code = '{args.tenant_code}' AND NOT se.deleted"
        f" AND se.family_role = 'proband' AND se.affected_status = 'affected' {seq_ids}"
    )
    consequences = ", ".join(f"'{c}'" for c in rs.scope.consequences)
    classified = (
        f"SELECT c.locus_id, c.transcript_id FROM {args.shared_db}.snv__consequence c"
        # VEP --merged flags both the Ensembl and the RefSeq copy of a MANE Select: keep the Ensembl one.
        f" WHERE {'TRUE' if args.all_transcripts else "c.is_mane_select AND c.transcript_id LIKE 'ENST%'"}"
        f" AND (arrays_overlap(c.consequences, [{consequences}]) OR c.spliceai_ds >= {rs.scope.spliceai_min})"
        f" AND c.locus_id IN (SELECT o.locus_id FROM {args.tenant_db}.germline__snv__occurrence o"
        f" WHERE o.seq_id IN ({cases}) {part})"
    )
    return {
        "shared_db": args.shared_db,
        "tenant_db": args.tenant_db,
        "output_db": args.output_db,
        "tenant_code": args.tenant_code,
        "clinical_db": args.clinical_db,
        "ruleset_version": rs.version,
        "prior": str(rs.priority.prior),
        "very_strong_odds": str(rs.priority.very_strong_odds),
        "pp4_blocked_by": "[" + ", ".join(f"'{c}'" for c in rs.pp4.blocked_by) + "]",
        "recessive": "[" +", ".join(f"'{c}'" for c in rs.inheritance_fit.recessive) + "]",
        "dominant": "[" + ", ".join(f"'{c}'" for c in rs.inheritance_fit.dominant) + "]",
        "single_het_recessive_points": str(rs.inheritance_fit.single_het_recessive_points),
        "healthy_parent_dominant_points": str(rs.inheritance_fit.healthy_parent_dominant_points),
        "pm1_window_bp": str(rs.pm1.window_bp),
        "clinvar_min_stars": str(rs.clinvar.min_stars),
        "min_an": str(rs.frequency.min_an),
        "parent_min_relatedness": str(rs.de_novo.parent_min_relatedness),
        "parent_max_ibs0_share": str(rs.de_novo.parent_max_ibs0_share),
        "cases": cases,
        "classified": classified,
        "occurrence_part": part,
        "comphet_part": part.replace("o.part", "ch.part"),
    }


def log(**fields) -> None:
    """Progress on stderr, flushed at once: it survives an OOM kill, unlike the final report."""
    print(json.dumps(fields), file=sys.stderr, flush=True)


def summary(result: pl.DataFrame, codes: str) -> dict:
    counts = result.select(pl.col(codes).explode().drop_nulls().value_counts())
    report = {"rows": result.height, "criteria": {r[codes][codes]: r[codes]["count"] for r in counts.to_dicts()}}
    if "acmg_class" in result.columns:
        report["classes"] = dict(result["acmg_class"].value_counts().iter_rows())
    return report


def worker(job: dict) -> dict:
    """One pod's job: one facts file of one step to one result file (paths are bucket/key)."""
    s3, rs, level = filesystem(), Ruleset.load(Path(job["ruleset"])), LEVELS[job["level"]]
    facts = pl.from_arrow(pq.read_table(job["input"], filesystem=s3))
    if job["level"] == "classification":
        result = combine(to_wide(facts, level["keys"]), rs, level["keys"]).collect()
        codes = "acmg_criteria"
    else:
        schema, names = {
            "variant": (VARIANT_FACT_SCHEMA, VARIANT_CRITERIA),
            "occurrence": (OCCURRENCE_FACT_SCHEMA, OCCURRENCE_CRITERIA),
        }[job["level"]]
        missing = set(schema) - set(facts.columns)
        if missing:
            raise ValueError(f"{job['level']} facts columns missing: {sorted(missing)}")
        result = evaluate(facts.select(list(schema)).cast(schema), rs, names, level["keys"]).collect()
        codes = "code"
    out = result.select(*level["keys"], *level["columns"])
    pq.write_table(out.to_arrow(), job["output"], filesystem=s3, compression="zstd")
    # ru_maxrss is in KiB on Linux.
    log(
        level=job["level"],
        file=job["output"].rsplit("/", 1)[-1],
        facts_rows=facts.height,
        peak_rss_mb=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024,
    )
    return summary(result, codes)


def check(args) -> dict:
    """EXPLAIN of the variant and occurrence facts queries (proves every Radiant table and column exists) and the
    number of cases of the scope; writes nothing. The classification query reads the output tables, created by the
    first run."""
    params = scope(args, Ruleset.load(Path(args.ruleset)))
    plans = {}
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM ({params['cases']}) c")
        plans["cases"] = cur.fetchone()[0]
        for name, facts in (("variant", "variant_facts.sql"), ("occurrence", "occurrence_facts.sql"),
                            ("phenotype", "phenotype_facts.sql")):
            cur.execute(f"EXPLAIN {sql(facts, **params)}")
            plans[name] = f"valid, {len(cur.fetchall())} plan lines"
    return plans


def read_text(s3, key: str) -> str:
    """A file under the bucket (or bucket/prefix) of S3_BUCKET."""
    with s3.open_input_stream(f"{s3_settings()['bucket']}/{key}") as f:
        return f.read().decode()


def load_hgnc(output_db: str, prefix: str, s3) -> pl.DataFrame:
    """Rebuilds the gene_symbol table from the HGNC file on S3, read by every gene join."""
    mapping = hgnc.symbols(read_text(s3, hgnc.FILE))
    pq.write_table(mapping.to_arrow(), f"{prefix}/hgnc/symbols.parquet", filesystem=s3)
    columns = ", ".join(hgnc.SCHEMA)
    execute(
        f"INSERT OVERWRITE {output_db}.gene_symbol ({columns})\nSELECT {columns}\n"
        f"FROM FILES(\n{files_properties(f's3://{prefix}/hgnc/symbols.parquet')}\n)"
    )
    return mapping


def load_clingen(output_db: str, prefix: str, s3, symbols: pl.DataFrame) -> int:
    """Rebuilds the clingen_gene table from the ClinGen files on S3, read by the variant facts."""
    genes = clingen.genes(read_text(s3, clingen.DOSAGE), read_text(s3, clingen.VALIDITY))
    genes = hgnc.normalize(genes, symbols).unique("symbol")
    pq.write_table(genes.to_arrow(), f"{prefix}/clingen/genes.parquet", filesystem=s3)
    columns = ", ".join(clingen.SCHEMA)
    execute(
        f"INSERT OVERWRITE {output_db}.clingen_gene ({columns})\nSELECT {columns}\n"
        f"FROM FILES(\n{files_properties(f's3://{prefix}/clingen/genes.parquet')}\n)"
    )
    return genes.height


def load_cspec(output_db: str, prefix: str, s3, symbols: pl.DataFrame) -> int:
    """Rebuilds the cspec_not_applicable table from the CSpec export on S3, read by the classification."""
    rows = hgnc.normalize(cspec.not_applicable(json.loads(read_text(s3, cspec.SPECS))), symbols).unique()
    pq.write_table(rows.to_arrow(), f"{prefix}/cspec/not_applicable.parquet", filesystem=s3)
    columns = ", ".join(cspec.SCHEMA)
    execute(
        f"INSERT OVERWRITE {output_db}.cspec_not_applicable ({columns})\nSELECT {columns}\n"
        f"FROM FILES(\n{files_properties(f's3://{prefix}/cspec/not_applicable.parquet')}\n)"
    )
    return rows.height


def load_protein(output_db: str) -> int:
    """Rebuilds the clinvar_protein table from the file on S3 (acmg.protein), read by the variant facts."""
    columns = ", ".join(protein.SCHEMA)
    execute(
        f"INSERT OVERWRITE {output_db}.clinvar_protein ({columns})\nSELECT {columns}\n"
        f"FROM FILES(\n{files_properties(f's3://{s3_settings()['bucket']}/{protein.FILE}')}\n)"
    )
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {output_db}.clinvar_protein")
        return cur.fetchone()[0]


def load_erepo(output_db: str, prefix: str, s3, rs: Ruleset) -> dict:
    """Rebuilds the clingen_erepo table from the ClinGen Evidence Repository export on S3, read by the VCEP step."""
    variants, unmapped = erepo.classifications(read_text(s3, erepo.FILE), rs)
    pq.write_table(variants.to_arrow(), f"{prefix}/erepo/variants.parquet", filesystem=s3)
    columns = ", ".join(erepo.SCHEMA)
    execute(
        f"INSERT OVERWRITE {output_db}.clingen_erepo ({columns})\nSELECT {columns}\n"
        f"FROM FILES(\n{files_properties(f's3://{prefix}/erepo/variants.parquet')}\n)"
    )
    return {"variants": variants.height, "unmapped_codes": dict(unmapped)}


def load_citations(output_db: str, prefix: str, s3) -> int:
    """Rebuilds the clinvar_citation table from var_citations.txt on S3, read by the priority."""
    variations = citations.variations(read_text(s3, citations.FILE))
    pq.write_table(variations.to_arrow(), f"{prefix}/clinvar_citations/variations.parquet", filesystem=s3)
    columns = ", ".join(citations.SCHEMA)
    execute(
        f"INSERT OVERWRITE {output_db}.clinvar_citation ({columns})\nSELECT {columns}\n"
        f"FROM FILES(\n{files_properties(f's3://{prefix}/clinvar_citations/variations.parquet')}\n)"
    )
    return variations.height


def replace_with_vcep(params: dict) -> dict:
    """Swaps the computed criteria of the variants a VCEP curated for the VCEP's (sql/vcep_criteria.sql)."""
    execute(sql("vcep_criteria.sql", **params))
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(DISTINCT locus_id), count(*) FROM {params['output_db']}.acmg_variant_criteria"
            f" WHERE source = 'clingen_vcep' AND locus_id IN (SELECT locus_id FROM ({params['classified']}) k)"
        )
        variants, criteria = cur.fetchone()
    return {"variants": variants, "criteria": criteria}


def load_phenotype(params: dict, rs: Ruleset, prefix: str, s3, symbols: pl.DataFrame) -> dict:
    """Rebuilds acmg_phenotype for the cases: the PP4 points of each gene, from the case's HPO terms."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql("phenotype_facts.sql", **params))
        patients = pl.DataFrame(cur.fetchall(), schema={"case": pl.Int64, "hpo_id": pl.String}, orient="row")
    closure, alt = hpo.ontology(read_text(s3, hpo.ONTOLOGY))
    terms = hpo.gene_terms(closure, hgnc.normalize(hpo.annotations(read_text(s3, hpo.GENES)), symbols))
    genes = (
        hpo.similarity(hpo.normalize(patients, alt), closure, terms)
        .with_columns(pp4_points=hpo.pp4_points(rs.pp4))
        .rename({"case": "seq_id"})
        .with_columns(ruleset_version=pl.lit(rs.version))
        .select(PHENOTYPE_COLUMNS)
    )
    pq.write_table(genes.to_arrow(), f"{prefix}/phenotype/genes.parquet", filesystem=s3)
    columns = ", ".join(PHENOTYPE_COLUMNS)
    execute(
        f"DELETE FROM {params['output_db']}.acmg_phenotype WHERE seq_id IN ({params['cases']});\n"
        f"INSERT INTO {params['output_db']}.acmg_phenotype ({columns})\nSELECT {columns}\n"
        f"FROM FILES(\n{files_properties(f's3://{prefix}/phenotype/genes.parquet')}\n)"
    )
    pp4 = genes["pp4_points"].value_counts().sort("pp4_points")
    return {"cases_with_terms": patients["case"].n_unique(), "case_genes_by_pp4_points": dict(pp4.iter_rows())}


def run_priority(params: dict, args) -> dict:
    session = f"SET query_timeout = {int(args.query_timeout)};\nSET insert_timeout = {int(args.query_timeout)};\n"
    execute(session + sql("priority.sql", **params))
    with connect() as conn, conn.cursor() as cur:
        report = {}
        for column in ("pp4_points", "inheritance_points"):
            cur.execute(
                f"SELECT {column}, count(*) FROM {params['output_db']}.acmg_priority"
                f" WHERE seq_id IN ({params['cases']}) GROUP BY {column} ORDER BY {column}"
            )
            report[f"occurrences_by_{column}"] = {int(p): n for p, n in cur.fetchall()}
        return report


def run_level(name: str, params: dict, args, prefix: str, s3) -> dict:
    level, timings = LEVELS[name], {}
    report = {"rows": 0, "criteria": Counter(), "classes": Counter()}
    timeout = int(args.query_timeout)
    session = f"SET query_timeout = {timeout};\nSET insert_timeout = {timeout};\n"

    t = time.perf_counter()
    facts = f"{prefix}/{name}/facts"
    target = f'"target_max_file_size" = "{args.file_size_mb * 1024 * 1024}",\n'
    unload = f"INSERT INTO FILES(\n{target}{files_properties(f's3://{facts}/')}\n)\n{sql(level['facts'], **params)}"
    execute(session + unload)
    files = [f for f in s3.get_file_info(fs.FileSelector(facts, recursive=True)) if f.type == fs.FileType.File]
    inputs = sorted(f.path for f in files)
    timings["unload_s"] = time.perf_counter() - t
    sizes = [f.size for f in files] or [0]
    log(
        level=name,
        unloaded_files=len(files),
        total_mb=sum(sizes) >> 20,
        max_file_mb=max(sizes) >> 20,
        unload_s=round(timings["unload_s"], 1),
    )

    t = time.perf_counter()
    jobs = [
        {"level": name, "input": path, "output": f"{prefix}/{name}/out/{i:05d}.parquet", "ruleset": args.ruleset}
        for i, path in enumerate(inputs)
    ]
    # A fresh process per file: Polars keeps its memory, so a reused worker's peak grows file after file.
    with ProcessPoolExecutor(max_workers=args.workers, max_tasks_per_child=1) as pool:
        for counts in pool.map(worker, jobs):
            report["rows"] += counts["rows"]
            report["criteria"].update(counts["criteria"])
            report["classes"].update(counts.get("classes", {}))
    timings["workers_s"] = time.perf_counter() - t

    t = time.perf_counter()
    table = f"{args.output_db}.{level['table']}"
    execute(f"{session}DELETE FROM {table} WHERE {Template(level['scope']).substitute(params)}")
    if jobs:
        columns = ", ".join([*level["keys"], *level["columns"]])
        execute(
            f"{session}INSERT INTO {table} ({columns})\nSELECT {columns}\n"
            f"FROM FILES(\n{files_properties(f's3://{prefix}/{name}/out/*.parquet')}\n)"
        )
    timings["load_s"] = time.perf_counter() - t
    return {
        "files": len(inputs),
        "rows": report["rows"],
        **({"classes": dict(report["classes"])} if report["classes"] else {}),
        "criteria": dict(sorted(report["criteria"].items())),
        "timings": {k: round(v, 2) for k, v in timings.items()},
    }


def run(args) -> dict:
    if args.output_db in (args.shared_db, args.tenant_db):
        raise ValueError(f"the output database must not be a pipeline database ({args.output_db})")
    s = s3_settings()
    prefix = f"{s['bucket']}/runs/{uuid.uuid4().hex[:8]}"
    s3 = filesystem()
    if not s["instance_profile"]:
        s3.create_dir(s["bucket"].split("/")[0])
    params = scope(args, Ruleset.load(Path(args.ruleset)))
    start = time.perf_counter()
    execute(sql("create_tables.sql", output_db=args.output_db))
    try:
        rs = Ruleset.load(Path(args.ruleset))
        symbols = load_hgnc(args.output_db, prefix, s3)
        log(hgnc_symbols=symbols.height, clingen_genes=load_clingen(args.output_db, prefix, s3, symbols))
        log(cspec_not_applicable=load_cspec(args.output_db, prefix, s3, symbols))
        log(clinvar_protein=load_protein(args.output_db))
        log(erepo=load_erepo(args.output_db, prefix, s3, rs))
        log(clinvar_citation_variations=load_citations(args.output_db, prefix, s3))
        steps = {}
        for name in LEVELS:
            steps[name] = run_level(name, params, args, prefix, s3)
            # The VCEP's criteria replace the computed ones once both criteria levels are loaded.
            if name == "occurrence" and not args.no_vcep:
                steps["vcep"] = replace_with_vcep(params)
        steps["phenotype"] = load_phenotype(params, rs, prefix, s3, symbols)
        steps["priority"] = run_priority(params, args)
    finally:
        if not args.keep_files:
            s3.delete_dir(prefix)
    return {**steps, "total_s": round(time.perf_counter() - start, 2)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output-db", default="acmg_poc_out", help="created if missing; not a pipeline database")
    cases = parser.add_mutually_exclusive_group()
    cases.add_argument("--part", type=int, help="the cases of one import part")
    cases.add_argument("--seq-ids", type=int, nargs="+", help="these sequencings (affected probands)")
    parser.add_argument("--workers", type=int, default=4, help="parallel workers (pods)")
    parser.add_argument("--file-size-mb", type=int, default=8, help="target size of each facts file")
    parser.add_argument("--ruleset", default=str(HERE / "ruleset.yaml"))
    parser.add_argument("--all-transcripts", action="store_true", help="every transcript, not only MANE Select")
    parser.add_argument("--keep-files", action="store_true", help="leave the run's files on S3")
    parser.add_argument("--no-vcep", action="store_true", help="keep the computed criteria of VCEP-curated variants")
    parser.add_argument("--check", action="store_true", help="EXPLAIN the facts queries only: no read, no write")
    parser.add_argument("--query-timeout", type=int, default=3600, help="seconds an unload or a load may run")
    parser.add_argument("--shared-db", default="acmg_poc")
    parser.add_argument("--tenant-db", default="acmg_poc")
    parser.add_argument("--tenant-code", default="acmg_poc", help="tenant_code of staging_sequencing_experiment")
    parser.add_argument("--clinical-db", default="radiant_jdbc.public", help="catalog.database of obs_categorical")
    args = parser.parse_args()
    print(json.dumps(check(args) if args.check else run(args), indent=2))


if __name__ == "__main__":
    main()
