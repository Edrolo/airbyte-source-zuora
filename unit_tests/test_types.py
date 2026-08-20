from source_zuora.zuora_types import TYPE_BOOL, TYPE_NUMBER, TYPE_STRING, json_type


def test_json_type_is_case_insensitive():
    # Zuora's Describe API returns "ZOQL" uppercase; Data Query returns lowercase.
    assert json_type("ZOQL") == json_type("zoql")


def test_json_type_maps_number():
    # "number" appears in Describe output and was missing from the map entirely.
    assert json_type("number") == TYPE_NUMBER


def test_json_type_known_scalars():
    assert json_type("decimal") == TYPE_NUMBER
    assert json_type("integer") == TYPE_NUMBER
    assert json_type("boolean") == TYPE_BOOL
    assert json_type("datetime") == TYPE_STRING
    assert json_type("picklist") == TYPE_STRING


def test_json_type_unknown_and_empty_default_to_string():
    assert json_type("wat") == TYPE_STRING
    assert json_type("") == TYPE_STRING
    assert json_type(None) == TYPE_STRING


def test_json_type_strips_whitespace():
    assert json_type("  decimal  ") == TYPE_NUMBER
