"""StarRocks access over MySQL and the SQL templates of `sql/`.

Connection settings come from the environment, never from the command line or a committed file:
STARROCKS_MYSQL_HOST, STARROCKS_MYSQL_PORT, STARROCKS_USER, STARROCKS_PASSWORD, STARROCKS_SSL_CA (TLS).
"""

import os
import re
from pathlib import Path
from string import Template

import pymysql

HERE = Path(__file__).parents[1]
# Template parameters that are SQL identifiers or literals: checked before substitution.
IDENTIFIERS = {
    "shared_db": re.compile(r"^\w+$"),
    "tenant_db": re.compile(r"^\w+$"),
    "output_db": re.compile(r"^\w+$"),
    "tenant_code": re.compile(r"^\w+$"),
    "clinical_db": re.compile(r"^\w+\.\w+$"),
}


def sql(name: str, **params) -> str:
    for key, pattern in IDENTIFIERS.items():
        if key in params and not pattern.fullmatch(params[key]):
            raise ValueError(f"invalid {key}: {params[key]!r}")
    return Template((HERE / "sql" / name).read_text()).substitute(params)


def statements(script: str) -> list[str]:
    lines = [line for line in script.splitlines() if not line.lstrip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def connect():
    ca = os.environ.get("STARROCKS_SSL_CA")
    return pymysql.connect(
        host=os.environ.get("STARROCKS_MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("STARROCKS_MYSQL_PORT", "19030")),
        user=os.environ.get("STARROCKS_USER", "root"),
        password=os.environ.get("STARROCKS_PASSWORD", ""),
        autocommit=True,
        # CA checked, hostname not: the certificate may not name the endpoint, as with the ETL's client.
        ssl={"ca": ca, "check_hostname": False} if ca else None,
    )


def execute(script: str) -> None:
    with connect() as conn, conn.cursor() as cur:
        for statement in statements(script):
            cur.execute(statement)
