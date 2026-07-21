#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from typing import Any, Dict, Mapping

from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator

from .zuora_endpoint import get_url_base


class ZuoraOauth2Authenticator(Oauth2Authenticator):
    """
    Zuora uses the OAuth2 `client_credentials` grant and has no refresh token,
    so the standard `refresh_token` arg is stripped from the refresh request body.
    """

    def build_refresh_request_body(self) -> Mapping[str, Any]:
        body = dict(super().build_refresh_request_body())
        body.pop("refresh_token", None)
        return body


class ZuoraAuthenticator:
    def __init__(self, config: Dict):
        self.config = config

    @property
    def url_base(self) -> str:
        return get_url_base(self.config["tenant_endpoint"])

    def get_auth(self) -> ZuoraOauth2Authenticator:
        return ZuoraOauth2Authenticator(
            token_refresh_endpoint=f"{self.url_base}/oauth/token",
            client_id=self.config["client_id"],
            client_secret=self.config["client_secret"],
            refresh_token=None,
            grant_type="client_credentials",
        )
