"""Local stack only: drops, creates and fills synthetic stand-ins of the Radiant tables the classifier reads.

Kept out of the `acmg` package and of the image: nothing that resets a table ships to a real cluster.
"""

import argparse
import json
import os
import re
import time
from pathlib import Path
from string import Template

from acmg.db import execute

SQL = Path(__file__).parent / "sql"
LOCAL_HOSTS = {"127.0.0.1", "localhost", "starrocks_acmg_poc"}


def sql(name: str, **params) -> str:
    return Template((SQL / name).read_text()).substitute(params)


def setup(args) -> dict:
    # Two locks: a local StarRocks, and the POC's own databases.
    host = os.environ.get("STARROCKS_MYSQL_HOST", "127.0.0.1")
    if host not in LOCAL_HOSTS:
        raise ValueError(f"setup only runs against a local StarRocks, not {host!r}")
    dbs = {"shared_db": args.shared_db, "tenant_db": args.tenant_db}
    if not all(re.fullmatch(r"acmg_poc\w*", db) for db in dbs.values()):
        raise ValueError("setup only writes to acmg_poc* databases")
    t = time.perf_counter()
    execute(sql("reset.sql", **dbs))
    execute(sql("schema.sql", **dbs))
    for first in range(1, args.n_loci + 1, args.slice_loci):
        last = min(first + args.slice_loci - 1, args.n_loci)
        execute(sql("seed.sql", **dbs, first=first, last=last))
        print(f"seeded loci {first}-{last} ({round(time.perf_counter() - t)} s)", flush=True)
    cohort = {"n_cases": int(args.n_cases), "variants_per_proband": int(args.variants_per_proband)}
    execute(sql("seed_cohort.sql", **dbs, **cohort, n_loci=int(args.n_loci), tenant_code=args.tenant_code))
    return {"n_loci": args.n_loci, **cohort, "seconds": round(time.perf_counter() - t, 2)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n-loci", type=int, default=200_000)
    parser.add_argument("--slice-loci", type=int, default=5_000_000, help="loci per INSERT")
    parser.add_argument("--n-cases", type=int, default=3_000, help="synthetic cases (80 %% trios)")
    parser.add_argument("--variants-per-proband", type=int, default=5_000)
    parser.add_argument("--shared-db", default="acmg_poc")
    parser.add_argument("--tenant-db", default="acmg_poc")
    parser.add_argument("--tenant-code", default="acmg_poc")
    print(json.dumps(setup(parser.parse_args()), indent=2))


if __name__ == "__main__":
    main()
