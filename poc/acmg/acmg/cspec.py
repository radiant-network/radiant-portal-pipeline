"""Criteria that ClinGen VCEPs declare not applicable in a gene, from the Criteria Specification Registry (CSpec).

`python -m acmg.cspec specs.json` downloads the released specifications (public) to copy under s3://<bucket>/cspec/.
"""

import json
import re
import sys
import urllib.request
from collections import defaultdict

import polars as pl

SPECS = "cspec/specs.json"
SCHEMA = {"symbol": pl.String, "criterion": pl.String}
BASE = "https://cspec.clinicalgenome.org/cspec"
REGISTRY = f"{BASE}/SequenceVariantInterpretation"
NA = {"not applicable", "not applicable for this vcep"}


def _applicable(code: dict) -> bool:
    """A code is applicable unless it says not applicable, or all its strengths do. A strength's `status` is not
    read: applicable strengths with their thresholds (PAH PM2_Supporting) are marked "not approved"."""
    strengths = [s for s in code.get("strengthDescriptor") or [] if (s.get("applicability") or "").lower() not in NA]
    return (code.get("applicability") or "").lower() not in NA and bool(strengths)


def not_applicable(specs: list[dict]) -> pl.DataFrame:
    """symbol, criterion (base code, e.g. PM1): every specification of the gene that lists the criterion says it is
    not applicable, and every specification of the gene lists it. A gene in several specifications keeps a criterion
    that one of them applies or leaves out."""
    specs_of = defaultdict(set)  # gene -> specs
    applicable = defaultdict(lambda: defaultdict(bool))  # (gene, criterion) -> spec -> applicable in it
    for spec in specs:
        for entry in spec["ld"].get("CriteriaCode", []):
            code = entry["entContent"]
            base = re.match(r"[A-Z]+\d", code["label"])
            for gene in code.get("gene") or []:
                specs_of[gene].add(spec["entId"])
                if base:
                    cell = applicable[(gene, base.group(0))]
                    cell[spec["entId"]] = cell[spec["entId"]] or _applicable(code)
    rows = [
        (gene, criterion) for (gene, criterion), by_spec in applicable.items()
        if set(by_spec) == specs_of[gene] and not any(by_spec.values())
    ]  # fmt: skip
    return pl.DataFrame(rows, schema=SCHEMA, orient="row").unique().sort(list(SCHEMA))


def _get(url: str) -> dict:
    request = urllib.request.Request(url, headers={"User-Agent": "acmg-poc"})
    with urllib.request.urlopen(request, timeout=300) as response:
        return json.load(response)


def _state(spec: dict) -> list[str]:
    return [s["name"] for s in spec["entContent"].get("states", []) if s.get("current")]


def fetch() -> list[dict]:
    """Every specification the registry lists as Released, at its newest released version (the current one can be a
    draft). The full list alone also holds superseded specifications (GN004, the RASopathy one before per-gene specs)."""
    released = {s["@id"].rsplit("/", 1)[1] for s in _get(f"{BASE}/api/svis")["data"] if s.get("status") == "Released"}
    specs = []
    for spec in _get(f"{REGISTRY}/id?detail=high&pgSize=1000&pg=1")["data"]:
        if spec["entId"] not in released:
            continue
        if _state(spec) == ["Released"]:
            specs.append(spec)
            continue
        for v in _get(f"{REGISTRY}/id/{spec['entId']}/version")["data"]:
            version = _get(f"{REGISTRY}/id/{spec['entId']}/version/{v['@id'].rsplit('/', 1)[1]}")["data"]
            version = version[0] if isinstance(version, list) else version
            if _state(version) == ["Released"]:
                specs.append(version)
                break
    return specs


if __name__ == "__main__":
    with open(sys.argv[1], "w") as f:
        json.dump(fetch(), f)
