import pytest
import requests

from source_zuora.zuora_errors import ZuoraConfigError, ZuoraTransientError
from source_zuora.zuora_http import ZuoraHttpClient

BASE = "https://rest.zuora.com"


class FakeAuth:
    def get_auth_header(self):
        return {"Authorization": "Bearer test"}


def make_http(**kwargs):
    kwargs.setdefault("backoff_factor", 0)
    return ZuoraHttpClient(BASE, FakeAuth(), **kwargs)


def test_headers_include_auth_and_content_type():
    headers = make_http().headers()
    assert headers["Authorization"] == "Bearer test"
    assert headers["Content-Type"] == "application/json"


def test_request_returns_ok_response(requests_mock):
    requests_mock.get(f"{BASE}/ping", json={"ok": True})
    assert make_http().request("GET", f"{BASE}/ping").json() == {"ok": True}


def test_request_retries_then_succeeds(requests_mock):
    requests_mock.get(f"{BASE}/ping", [{"status_code": 503}, {"json": {"ok": True}}])
    assert make_http().request("GET", f"{BASE}/ping").json() == {"ok": True}


def test_request_raises_transient_after_exhausting_retries(requests_mock):
    requests_mock.get(f"{BASE}/ping", status_code=503)
    with pytest.raises(ZuoraTransientError):
        make_http(max_retries=1).request("GET", f"{BASE}/ping")


def test_request_raises_config_error_on_401(requests_mock):
    requests_mock.get(f"{BASE}/ping", status_code=401)
    with pytest.raises(ZuoraConfigError):
        make_http().request("GET", f"{BASE}/ping")


def test_request_raises_config_error_on_403(requests_mock):
    requests_mock.get(f"{BASE}/ping", status_code=403)
    with pytest.raises(ZuoraConfigError):
        make_http().request("GET", f"{BASE}/ping")


def test_request_retries_connection_errors(requests_mock):
    requests_mock.get(
        f"{BASE}/ping",
        [{"exc": requests.exceptions.ConnectionError}, {"json": {"ok": True}}],
    )
    assert make_http().request("GET", f"{BASE}/ping").json() == {"ok": True}
