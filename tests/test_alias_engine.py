"""Regression tests for the alias/matching engine (modality precision).

No database required: server/facilities.py is loaded with a stubbed
server.sql, so every check exercises the real production matching code.

Run standalone:  python tests/test_alias_engine.py
Or with pytest:   pytest tests/test_alias_engine.py

Background (defect fixed 2026-11-26): the dialysis family used to be one
symmetric alias group mixing generic dialysis with every modality, and
_expand_variants substituted to a fixpoint, producing composites like
"peritoneal peritoneal dialysis". Every dialysis-family query then matched
the same 1,055 records. Now each modality has its own equivalence group,
broad family discovery is one-directional from the generic phrase, and
substitution is single-pass — these tests pin that behavior.
"""
import importlib.util
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
APP = os.path.dirname(HERE)


def _load_facilities():
    pkg = types.ModuleType("server")
    pkg.__path__ = []
    sys.modules.setdefault("server", pkg)
    sqlstub = types.ModuleType("server.sql")
    sqlstub.run_query = lambda *a, **k: []
    sqlstub.run_query_pair = lambda *a, **k: ([], [])
    sys.modules["server.sql"] = sqlstub
    spec = importlib.util.spec_from_file_location(
        "server.facilities", os.path.join(APP, "server", "facilities.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


F = _load_facilities()


def _v(query):
    return F._expand_variants(F._norm_text(query))


def _no_doubled_tokens(variants):
    """No variant may contain the same word twice in a row (the signature of
    a recursive replacement artifact like 'peritoneal peritoneal dialysis')."""
    for v in variants:
        toks = v.split()
        for a, b in zip(toks, toks[1:]):
            if a == b:
                return False
    return True


# ---------------------------------------------------------------- dialysis
def test_crrt_matches_only_crrt_family():
    """CRRT means CRRT / continuous renal replacement therapy — nothing else."""
    assert set(_v("crrt")) == {"crrt", "continuous renal replacement therapy", "crrts"}


def test_peritoneal_dialysis_never_broadens():
    """Peritoneal dialysis is a distinct modality: it matches itself only."""
    assert _v("peritoneal dialysis") == ["peritoneal dialysis"]


def test_hemodialysis_spelling_equivalence_only():
    """hemodialysis <-> haemodialysis are orthographic variants of ONE modality."""
    assert set(_v("hemodialysis")) == {"hemodialysis", "haemodialysis"}
    assert set(_v("haemodialysis")) == {"haemodialysis", "hemodialysis"}


def test_hemofiltration_spelling_equivalence_only():
    assert set(_v("hemofiltration")) == {
        "hemofiltration", "haemofiltration", "hemofiltrations", "haemofiltrations"}


def test_generic_dialysis_keeps_broad_one_directional_discovery():
    """The generic phrase still discovers the whole family (quick-search
    button behavior preserved) — but ONLY the generic phrase does."""
    vs = set(_v("dialysis"))
    assert "dialysis" in vs
    for fam in F.DIALYSIS_MODALITY_PHRASES:
        assert fam in vs, fam


def test_modifiers_preserved_in_discovery():
    """Patient-scope modifiers stay attached to every discovered variant."""
    vs = set(_v("pediatric dialysis"))
    assert "pediatric peritoneal dialysis" in vs
    assert "pediatric hemodialysis" in vs
    assert "pediatric crrt" in vs
    assert "paediatric dialysis" in vs  # modifier spelling equivalence, single-pass
    for v in vs:
        if ("hemodialysis" in v or "peritoneal" in v or "crrt" in v
                or "hemofiltration" in v):
            assert v.startswith(("pediatric", "paediatric")), v


def test_modifier_on_modality_does_not_broaden():
    """A modality query with a modifier stays locked to that modality."""
    assert set(_v("pediatric hemodialysis")) == {
        "pediatric hemodialysis", "paediatric hemodialysis", "pediatric haemodialysis"}


# ------------------------------------------------------------ anti-artifact
def test_no_recursive_replacement_artifacts():
    queries = [
        "dialysis", "crrt", "peritoneal dialysis", "hemodialysis", "hemofiltration",
        "pediatric dialysis", "paediatric dialysis", "physio therapy", "physio",
        "cesarean", "cesarean section", "physiotherapy", "cardiac surgery",
        "burn care", "mri", "nicu",
    ]
    for q in queries:
        vs = _v(q)
        assert _no_doubled_tokens(vs), q
        for v in vs:
            assert "peritoneal peritoneal" not in v, (q, v)
            assert "therapy therapy" not in v, (q, v)
            assert "section section" not in v, (q, v)
            assert "dialysis dialysis" not in v, (q, v)


def test_longest_member_first_prevents_carving():
    """'physio' must not carve 'physio therapy' into 'physiotherapy therapy'."""
    assert set(_v("physio therapy")) == {
        "physio therapy", "physiotherapy", "physical therapy", "physio", "physios"}


# ------------------------------------------------------------ structure sync
def test_dialysis_family_definition_mirrors_modality_groups():
    """DIALYSIS_FAMILY must stay in sync with the four modality groups in
    ALIAS_GROUPS (generic dialysis + every group member), and those groups
    must be pairwise disjoint."""
    merged = {m for g in F.ALIAS_GROUPS[:4] for m in g}
    assert merged == F.DIALYSIS_FAMILY - {"dialysis"}
    groups = [set(g) for g in F.ALIAS_GROUPS[:4]]
    for i in range(len(groups)):
        for j in range(i + 1, len(groups)):
            assert not (groups[i] & groups[j])


def test_maps_to_dialysis_family_unchanged():
    """Enrichment attachment scope is unchanged by the refactor."""
    for q in ["dialysis", "crrt", "peritoneal dialysis", "hemodialysis",
              "continuous renal replacement therapy", "hemofiltration",
              "pediatric dialysis", "paediatric crrt"]:
        assert F._maps_to_dialysis_family(F._norm_text(q)), q
    for q in ["mri", "cardiac surgery", "burn care", "cataract surgery"]:
        assert not F._maps_to_dialysis_family(F._norm_text(q)), q
    # A non-canonical phrasing is plain text search, not a family claim:
    assert not F._maps_to_dialysis_family(F._norm_text("kidney dialysis"))


# --------------------------------------------------- other services stable
def test_other_services_variant_sets_stable():
    assert set(_v("physiotherapy")) == {
        "physiotherapy", "physical therapy", "physio therapy", "physio", "physios"}
    assert "c section" in set(_v("cesarean"))
    assert "lscs" in set(_v("cesarean"))
    assert "neonatal intensive care" in set(_v("nicu"))
    assert "magnetic resonance imaging" in set(_v("mri"))
    assert "open heart surgery" in set(_v("cardiac surgery"))
    assert "burn unit" in set(_v("burn care"))


if __name__ == "__main__":
    tests = [fn for name, fn in sorted(globals().items())
             if name.startswith("test_") and callable(fn)]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"PASS {fn.__name__}")
        except AssertionError as exc:
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
    print(f"\n{len(tests) - failed}/{len(tests)} alias-engine regression tests passed")
    sys.exit(1 if failed else 0)