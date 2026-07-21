#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import json
import time
from typing import Any, Iterator, List, Mapping, MutableMapping, Optional

import requests

from .zuora_errors import ZOQLQueryCannotProcessObject, ZOQLQueryFailed

TYPE_NUMBER = ["number", "null"]
TYPE_STRING = ["string", "null"]
TYPE_OBJECT = ["object", "null"]
TYPE_ARRAY = ["array", "null"]
TYPE_BOOL = ["boolean", "null"]

TYPE_MAPPING = {
    "decimal(22,9)": TYPE_NUMBER,
    "decimal": TYPE_NUMBER,
    "integer": TYPE_NUMBER,
    "int": TYPE_NUMBER,
    "bigint": TYPE_NUMBER,
    "smallint": TYPE_NUMBER,
    "double": TYPE_NUMBER,
    "float": TYPE_NUMBER,
    "timestamp": TYPE_NUMBER,
    "date": TYPE_STRING,
    "datetime": TYPE_STRING,
    "timestamp with time zone": TYPE_STRING,
    "picklist": TYPE_STRING,
    "text": TYPE_STRING,
    "varchar": TYPE_STRING,
    "zoql": TYPE_OBJECT,
    "binary": TYPE_OBJECT,
    "json": TYPE_OBJECT,
    "xml": TYPE_OBJECT,
    "blob": TYPE_OBJECT,
    "list": TYPE_ARRAY,
    "array": TYPE_ARRAY,
    "boolean": TYPE_BOOL,
    "bool": TYPE_BOOL,
}

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
    ):
        self._url_base = url_base
        self._auth = authenticator
        self._data_query = data_query
        self._poll_interval = poll_interval
        self._session = session or requests.Session()

    def _headers(self) -> Mapping[str, str]:
        return {**self._auth.get_auth_header(), "Content-Type": "application/json"}

    def _base_params(self) -> MutableMapping[str, Any]:
        params: MutableMapping[str, Any] = {
            "compression": "NONE",
            "output": {"target": "S3"},
            "outputFormat": "JSON",
        }
        if self._data_query == "Unlimited":
            params["sourceData"] = "DATAHUB"
        return params

    def submit_job(self, zoql: str) -> str:
        params = self._base_params()
        params["query"] = zoql
        resp = self._session.post(
            f"{self._url_base}/query/jobs", headers=self._headers(), json=params
        )
        resp.raise_for_status()
        return resp.json()["data"]["id"]

    def poll_job(self, job_id: str) -> str:
        while True:
            resp = self._session.get(
                f"{self._url_base}/query/jobs/{job_id}", headers=self._headers()
            )
            resp.raise_for_status()
            data = resp.json()["data"]
            status = data["queryStatus"]
            if status == "completed":
                return data["dataFile"]
            if status in _ERROR_STATUSES:
                message = data.get("errorMessage", "") or ""
                if _PROCESS_OBJECT_ERROR in message:
                    raise ZOQLQueryCannotProcessObject(message)
                raise ZOQLQueryFailed(message, data.get("query", ""))
            time.sleep(self._poll_interval)

    def _download(self, data_file_url: str) -> Iterator[Mapping[str, Any]]:
        resp = self._session.get(data_file_url)
        resp.raise_for_status()
        for line in resp.text.splitlines():
            if line.strip():
                yield json.loads(line)

    def run_query(self, zoql: str) -> Iterator[Mapping[str, Any]]:
        job_id = self.submit_job(zoql)
        data_file_url = self.poll_job(job_id)
        yield from self._download(data_file_url)

    def list_objects(self) -> List[str]:
        return [row["Table"] for row in self.run_query("SHOW TABLES")]

    def describe_object(self, name: str) -> Mapping[str, Mapping[str, Any]]:
        return {
            row["Column"]: {"type": TYPE_MAPPING.get(row.get("Type"), TYPE_STRING)}
            for row in self.run_query(f"DESCRIBE {name}")
        }
