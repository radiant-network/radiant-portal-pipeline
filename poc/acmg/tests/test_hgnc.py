import polars as pl
from acmg import hgnc

TEXT = (
    "hgnc_id\tsymbol\tprev_symbol\n"
    "HGNC:1\tAFG2B\tSPATA5L1\n"
    "HGNC:2\tGBA1\t\"GLUC|GBA\"\n"
    "HGNC:3\tCPAP\t\"CENPJ|SHARED\"\n"
    "HGNC:4\tDRC4\t\"SHARED|GBA1X|CPAP\"\n"
)


def test_symbols_maps_current_and_unambiguous_previous():
    m = dict(hgnc.symbols(TEXT).iter_rows())
    assert m["AFG2B"] == "AFG2B"
    assert m["SPATA5L1"] == "AFG2B"
    assert m["GBA"] == "GBA1"
    assert m["CENPJ"] == "CPAP"
    # Two genes had it: no guess.
    assert "SHARED" not in m
    # A previous symbol that is another gene's current one stays that gene's.
    assert m["CPAP"] == "CPAP"


def test_normalize_keeps_unknown_symbols():
    df = pl.DataFrame({"symbol": ["SPATA5L1", "MT-CO1"], "x": [1, 2]})
    out = hgnc.normalize(df, hgnc.symbols(TEXT))
    assert out.sort("x")["symbol"].to_list() == ["AFG2B", "MT-CO1"]
