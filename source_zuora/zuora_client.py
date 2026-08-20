#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import json
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Iterator, List, Mapping, MutableMapping, Optional

import requests

from .zuora_errors import (
    ZOQLQueryCannotProcessObject,
    ZOQLQueryFailed,
    ZuoraTransientError,
    is_transient_job_error,
)
from .zuora_http import ZuoraHttpClient
from .zuora_types import json_type

_ERROR_STATUSES = {"failed", "canceled", "aborted"}
_PROCESS_OBJECT_ERROR = "process object"

class ZuoraQueryClient:
    """
    Runs ZOQL Data Query jobs against the Zuora REST API using the
    submit -> poll -> download (JSONL) workflow. Owns all HTTP; the CDK
    streams call this rather than driving requests themselves.
    """

    def __init__(
        self,
        url_base: str,
        authenticator: Any,
        data_query: str = "Live",
        poll_interval: float = 1.0,
        session: Optional[requests.Session] = None,
        max_poll_attempts: int = 1800,
        request_timeout: tuple = (30, 300),
        max_retries: int = 5,
        max_job_retries: int = 3,
        backoff_factor: float = 1.0,
        describe_concurrency: int = 10,
    ):
        self._url_base = url_base
        self._auth = authenticator
        self._data_query = data_query
        self._poll_interval = poll_interval
        self._max_poll_attempts = max_poll_attempts
        self._max_job_retries = max_job_retries
        self._describe_concurrency = describe_concurrency
        self._http = ZuoraHttpClient(
            url_base=url_base,
            authenticator=authenticator,
            session=session,
            request_timeout=request_timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )
        self._describe_cache: dict = {}

    def _headers(self) -> Mapping[str, str]:
        return self._http.headers()

    def _base_params(self) -> MutableMapping[str, Any]:
        params: MutableMapping[str, Any] = {
            "compression": "NONE",
            "output": {"target": "S3"},
            "outputFormat": "JSON",
        }
        if self._data_query == "Unlimited":
            params["sourceData"] = "DATAHUB"
        return params

    def _sleep_before_retry(self, attempt: int, response=None) -> None:
        self._http.sleep_before_retry(attempt, response)

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        return self._http.request(method, url, **kwargs)

    def submit_job(self, zoql: str) -> str:
        params = self._base_params()
        params["query"] = zoql
        response = self._request(
            "POST", f"{self._url_base}/query/jobs", headers=self._headers(), json=params
        )
        return response.json()["data"]["id"]

    def poll_job(self, job_id: str) -> str:
        attempts = 0
        while True:
            attempts += 1
            if attempts > self._max_poll_attempts:
                raise ZOQLQueryFailed(
                    f"Polling timed out after {self._max_poll_attempts} attempts", ""
                )
            response = self._request(
                "GET", f"{self._url_base}/query/jobs/{job_id}", headers=self._headers()
            )
            data = response.json()["data"]
            status = data["queryStatus"]
            if status == "completed":
                return data["dataFile"]
            if status in _ERROR_STATUSES:
                message = data.get("errorMessage", "") or ""
                if _PROCESS_OBJECT_ERROR in message:
                    raise ZOQLQueryCannotProcessObject(message)
                if is_transient_job_error(message):
                    raise ZuoraTransientError(f"Zuora Data Query job failed transiently: {message}")
                raise ZOQLQueryFailed(message, data.get("query", ""))
            time.sleep(self._poll_interval)

    def _download(self, data_file_url: str) -> Iterator[Mapping[str, Any]]:
        response = self._request("GET", data_file_url, stream=True)
        for line in response.iter_lines():
            if line:
                yield json.loads(line)

    def run_query(self, zoql: str) -> Iterator[Mapping[str, Any]]:
        # Retry the whole submit -> poll cycle on transient failures (HTTP transport
        # exhaustion or a transient job outage). Only the pre-download phase is retried,
        # so no partially-yielded records are ever duplicated.
        for attempt in range(self._max_job_retries + 1):
            try:
                job_id = self.submit_job(zoql)
                data_file_url = self.poll_job(job_id)
            except ZuoraTransientError:
                if attempt >= self._max_job_retries:
                    raise
                self._sleep_before_retry(attempt, None)
                continue
            yield from self._download(data_file_url)
            return

    def list_objects(self) -> List[str]:
        return [row["Table"] for row in self.run_query("SHOW TABLES")]

    def describe_object(self, name: str) -> Mapping[str, Mapping[str, Any]]:
        if name in self._describe_cache:
            return self._describe_cache[name]
        result = {
            row["Column"]: {"type": json_type(row.get("Type"))}
            for row in self.run_query(f"DESCRIBE {name}")
        }
        self._describe_cache[name] = result
        return result

    def warm_describe_cache(self, names: List[str]) -> None:
        """
        Pre-populate the describe cache for many objects concurrently.

        Each object's schema needs a separate ZOQL DESCRIBE job (submit -> poll ->
        download). Fetching them one at a time makes discovery of a large tenant
        exceed Airbyte's discover timeout, so pre-fetch them with a bounded thread
        pool (bounded to respect Zuora's Data Query concurrency limits). The first
        failure is re-raised, matching the sequential behavior.
        """
        pending = [name for name in names if name not in self._describe_cache]
        if not pending:
            return
        if self._describe_concurrency <= 1 or len(pending) == 1:
            for name in pending:
                self.describe_object(name)
            return
        pool = ThreadPoolExecutor(max_workers=self._describe_concurrency)
        try:
            futures = [pool.submit(self.describe_object, name) for name in pending]
            for future in futures:
                future.result()  # re-raise the first failure, if any
        finally:
            # cancel_futures: on a failure, drop not-yet-started DESCRIBEs instead of
            # running all of them before the error surfaces (fail fast).
            pool.shutdown(wait=True, cancel_futures=True)
