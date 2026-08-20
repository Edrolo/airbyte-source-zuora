#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import time
from typing import Any, Mapping, Optional

import requests

from .zuora_errors import ZuoraConfigError, ZuoraTransientError

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
RETRYABLE_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


class ZuoraHttpClient:
    """
    HTTP transport shared by the query backends: auth headers, retry with
    exponential backoff (honouring `Retry-After`), and classification of
    permanent auth failures. Owns no Zuora semantics beyond that.
    """

    def __init__(
        self,
        url_base: str,
        authenticator: Any,
        session: Optional[requests.Session] = None,
        request_timeout: tuple = (30, 300),
        max_retries: int = 5,
        backoff_factor: float = 1.0,
    ):
        self.url_base = url_base
        self._auth = authenticator
        self._session = session or requests.Session()
        self._request_timeout = request_timeout
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor

    def headers(self) -> Mapping[str, str]:
        return {**self._auth.get_auth_header(), "Content-Type": "application/json"}

    def sleep_before_retry(self, attempt: int, response: Optional[requests.Response]) -> None:
        delay = self._backoff_factor * (2**attempt)
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = float(retry_after)
                except ValueError:
                    pass
        if delay > 0:
            time.sleep(delay)

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", self._request_timeout)
        for attempt in range(self._max_retries + 1):
            try:
                response = self._session.request(method, url, **kwargs)
            except RETRYABLE_EXCEPTIONS as exc:
                if attempt >= self._max_retries:
                    raise ZuoraTransientError(
                        f"Request {method} {url} failed after {self._max_retries} retries: {exc}"
                    )
                self.sleep_before_retry(attempt, None)
                continue
            if response.status_code in RETRYABLE_STATUS:
                if attempt >= self._max_retries:
                    raise ZuoraTransientError(
                        f"Request {method} {url} failed with HTTP {response.status_code} "
                        f"after {self._max_retries} retries"
                    )
                self.sleep_before_retry(attempt, response)
                continue
            if response.status_code in (401, 403):
                raise ZuoraConfigError(
                    f"Zuora returned HTTP {response.status_code} for {url} — "
                    f"check your client credentials and API user permissions."
                )
            response.raise_for_status()
            return response
        raise ZuoraTransientError(f"Request {method} {url} exhausted retries")  # defensive
