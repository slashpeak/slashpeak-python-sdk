import pytest

from conftest import load_spec
from slashpeak.bigquery.mask import BUILTIN_RULES, CUSTOM_TOKEN, Masker, MaskError, builtin_masker, masker_from_config

SPEC = load_spec("mask_vectors.json")


def test_builtin_rules_match_the_router():
    assert [{"pattern": p, "token": t} for p, t in BUILTIN_RULES] == SPEC["builtinRules"]
    assert CUSTOM_TOKEN == SPEC["customToken"]


@pytest.mark.parametrize("vector", SPEC["vectors"], ids=lambda v: v["sql"][:30] or "empty")
def test_shared_vectors(vector):
    assert builtin_masker(vector.get("patterns", [])).mask(vector["sql"]) == vector["masked"]


def test_masking_masked_text_changes_nothing():
    m = builtin_masker()
    for vector in SPEC["vectors"]:
        once = m.mask(vector["sql"])
        assert m.mask(once) == once


def test_config_rules_are_used_in_order():
    m = masker_from_config({"revision": "abc", "rules": [{"pattern": "secret", "token": "<x>"}]})
    assert m.mask("a secret `secret`") == "a <x> `secret`"
    assert m.revision == "abc"


def test_bad_rule_raises_and_bad_block_is_none():
    with pytest.raises(MaskError):
        Masker([("(", "<x>")], "r")
    assert masker_from_config(None) is None
    assert masker_from_config({"revision": 1, "rules": []}) is None
    assert masker_from_config({"revision": "r", "rules": [{"pattern": 1}]}) is None


def test_unicode_digits_are_not_digits():
    # re.ASCII: Arabic-Indic digits are not \d in RE2 or BigQuery.
    assert builtin_masker().mask("x = '٥٥٥ ١٢٣ ٤٥٦٧'") == "x = '٥٥٥ ١٢٣ ٤٥٦٧'"
