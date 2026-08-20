import pytest

from source_zuora.zuora_backend import AQUA, DATA_QUERY, QueryBackend, get_backend
from source_zuora.zuora_client import ZuoraQueryClient

BASE = "https://rest.zuora.com"


class FakeAuth:
    def get_auth_header(self):
        return {"Authorization": "Bearer test"}


def test_data_query_client_implements_the_interface():
    assert issubclass(ZuoraQueryClient, QueryBackend)


def test_get_backend_defaults_to_data_query_when_key_absent():
    assert isinstance(get_backend({}, FakeAuth(), BASE), ZuoraQueryClient)


def test_get_backend_selects_data_query_explicitly():
    assert isinstance(get_backend({"query_api": DATA_QUERY}, FakeAuth(), BASE), ZuoraQueryClient)


def test_get_backend_passes_data_query_mode_through():
    assert get_backend({"data_query": "Unlimited"}, FakeAuth(), BASE)._data_query == "Unlimited"


def test_get_backend_rejects_unknown_query_api():
    with pytest.raises(ValueError, match="Unknown query_api"):
        get_backend({"query_api": "Telepathy"}, FakeAuth(), BASE)


def test_aqua_constant_is_the_spec_enum_value():
    assert AQUA == "AQuA"


def test_get_backend_selects_aqua():
    from source_zuora.zuora_aqua_client import ZuoraAquaClient

    assert isinstance(get_backend({"query_api": AQUA}, FakeAuth(), BASE), ZuoraAquaClient)
