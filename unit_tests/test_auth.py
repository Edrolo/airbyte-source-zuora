from source_zuora.zuora_auth import ZuoraAuthenticator


CONFIG = {
    "tenant_endpoint": "US Production",
    "client_id": "cid",
    "client_secret": "secret",
}


def test_url_base_maps_tenant():
    assert ZuoraAuthenticator(CONFIG).url_base == "https://rest.zuora.com"


def test_refresh_body_is_client_credentials_without_refresh_token():
    auth = ZuoraAuthenticator(CONFIG).get_auth()
    body = auth.build_refresh_request_body()
    assert body["grant_type"] == "client_credentials"
    assert body["client_id"] == "cid"
    assert body["client_secret"] == "secret"
    assert "refresh_token" not in body
