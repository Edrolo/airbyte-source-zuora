import json
import pathlib

from source_zuora.zuora_backend import AQUA, DATA_QUERY

SPEC = json.loads((pathlib.Path(__file__).parent.parent / "source_zuora" / "spec.json").read_text())
PROPERTIES = SPEC["connectionSpecification"]["properties"]


def test_query_api_property_exists_with_both_backends():
    assert PROPERTIES["query_api"]["enum"] == [DATA_QUERY, AQUA]


def test_query_api_defaults_to_data_query():
    assert PROPERTIES["query_api"]["default"] == DATA_QUERY


def test_query_api_is_optional_so_saved_configs_stay_valid():
    assert "query_api" not in SPEC["connectionSpecification"]["required"]


def test_data_query_description_scopes_itself_to_the_data_query_backend():
    assert "Data Query" in PROPERTIES["data_query"]["description"]
