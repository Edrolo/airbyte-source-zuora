import json
import pathlib

from source_zuora.zuora_endpoint import ZUORA_TENANT_ENDPOINT_MAP, get_url_base

SPEC_PATH = pathlib.Path(__file__).parent.parent / "source_zuora" / "spec.json"


def test_apac_endpoints_resolve():
    assert get_url_base("APAC Production") == "https://rest.ap.zuora.com"
    assert get_url_base("APAC Central Sandbox") == "https://rest.test.ap.zuora.com"


def test_unknown_endpoint_returns_none():
    assert get_url_base("Mars Sandbox") is None


def test_spec_enum_and_endpoint_map_are_in_sync():
    """Every tenant_endpoint the spec offers must resolve to a base URL (no drift)."""
    spec = json.loads(SPEC_PATH.read_text())
    enum = spec["connectionSpecification"]["properties"]["tenant_endpoint"]["enum"]
    unmapped = [value for value in enum if get_url_base(value) is None]
    assert unmapped == [], f"spec enum values missing from endpoint map: {unmapped}"
    # And every mapped endpoint is offered in the spec (no orphan map entries).
    missing_from_spec = [name for name in ZUORA_TENANT_ENDPOINT_MAP if name not in enum]
    assert missing_from_spec == [], f"endpoint map entries missing from spec enum: {missing_from_spec}"
