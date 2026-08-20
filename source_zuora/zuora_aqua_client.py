#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""
AQuA (Aggregate Query API) backend: Export ZOQL over `POST /v1/batch-query/`.

Differs from the Data Query backend in every layer — XML schema discovery, CSV
output, its own datetime literal, and foreign keys reached through relationships
rather than columns. See docs/superpowers/specs/2026-08-20-zuora-aqua-backend-design.md.
"""

import csv
import io
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple

import requests

from .zuora_backend import QueryBackend
from .zuora_describe import (
    foreign_key_columns,
    parse_fields,
    parse_object_names,
    parse_relationships,
)
from .zuora_errors import (
    ZOQLQueryCannotProcessObject,
    ZOQLQueryFailed,
    ZuoraTransientError,
    is_transient_job_error,
)
from .zuora_http import ZuoraHttpClient
from .zuora_types import TYPE_STRING, json_type

_DONE_STATUS = "completed"
_ERROR_STATUSES = {"aborted", "cancelled", "error", "failed"}

# An object with no export-context fields aborts with this; treated like the Data
# Query backend's "process object" failure, i.e. skip the stream, keep syncing.
_NO_SELECT_MARKER = "you must specify a select"


def render_query(
    obj: str,
    foreign_keys: Iterable[str],
    cursor: Optional[str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> str:
    """
    Render an Export ZOQL query: `select *` for the object's own export-context
    fields, plus `<Relationship>.Id` for each usable relationship, since `select *`
    alone returns no foreign keys.

    Bounds are rendered with `datetime.isoformat()`. A space-separated literal is
    accepted by Zuora but silently drops the predicate and returns the whole table,
    so this format is not optional.
    """
    columns = ", ".join(["*"] + [f"{relationship}.Id" for relationship in foreign_keys])
    query = f"select {columns} from {obj}"
    if not (cursor and start and end):
        return query
    return (
        f"{query} where "
        f"{cursor} >= '{start.isoformat()}' and "
        f"{cursor} <= '{end.isoformat()}' "
        f"order by {cursor} asc"
    )


def _coerce(value: str, json_schema_type: List[str]) -> Any:
    if "boolean" in json_schema_type:
        return value.strip().lower() == "true"
    if "number" in json_schema_type:
        try:
            number = float(value)
        except ValueError:
            return value
        return int(number) if number.is_integer() else number
    return value


def normalize_row(
    obj: str,
    row: Mapping[str, Optional[str]],
    types: Mapping[str, List[str]],
) -> Dict[str, Any]:
    """
    Turn one AQuA CSV row into an Airbyte record.

    Headers are `Prefix.Field`. A prefix equal to the queried object marks an own
    field, so the prefix is dropped; any other prefix is a relationship, so both
    parts are joined to give the foreign key its Data Query name
    (`Account.Id` -> `accountid`). No object has a relationship named after itself,
    so the two cases never collide.
    """
    record: Dict[str, Any] = {}
    for header, value in row.items():
        if not isinstance(header, str):
            continue
        if "." not in header:
            record[header.lower()] = value or None
            continue
        prefix, _, field = header.partition(".")
        key = field.lower() if prefix.lower() == obj.lower() else f"{prefix}{field}".lower()
        if value is None or value == "":
            record[key] = None
            continue
        record[key] = _coerce(value, list(types.get(key, TYPE_STRING)))
    return record


def _first_reason(data: Mapping[str, Any]) -> str:
    reasons = data.get("reasons") or []
    return reasons[0].get("message", "") if reasons else ""


def _job_message(data: Mapping[str, Any], batches: List[Mapping[str, Any]]) -> str:
    for batch in batches:
        message = batch.get("message")
        if message:
            return message
    return data.get("message") or ""


def _canonical_field(fields: Mapping[str, str], cursor: Optional[str]) -> Optional[str]:
    """Resolve a lowercase cursor name back to its canonical Export ZOQL spelling."""
    if not cursor:
        return None
    for field in fields:
        if field.lower() == cursor.lower():
            return field
    return cursor


class ZuoraAquaClient(QueryBackend):
    """
    Runs Export ZOQL exports through the AQuA submit -> poll -> download (CSV)
    workflow, and discovers schemas through the XML Describe API.
    """

    def __init__(
        self,
        url_base: str,
        authenticator: Any,
        session: Optional[requests.Session] = None,
        poll_interval: float = 1.0,
        max_poll_attempts: int = 1800,
        request_timeout: tuple = (30, 300),
        max_retries: int = 5,
        max_job_retries: int = 3,
        backoff_factor: float = 1.0,
        describe_concurrency: int = 10,
    ):
        self._http = ZuoraHttpClient(
            url_base=url_base,
            authenticator=authenticator,
            session=session,
            request_timeout=request_timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )
        self._poll_interval = poll_interval
        self._max_poll_attempts = max_poll_attempts
        self._max_job_retries = max_job_retries
        self._describe_concurrency = describe_concurrency
        self._canonical: Dict[str, str] = {}
        # lowercase object name -> (canonical fields, {relationship: fk column})
        self._describe_cache: Dict[str, Tuple[Dict[str, str], Dict[str, str]]] = {}

    @property
    def _url_base(self) -> str:
        return self._http.url_base

    # --- discovery ---

    def list_objects(self) -> List[str]:
        response = self._http.request(
            "GET", f"{self._url_base}/v1/describe", headers=self._http.headers()
        )
        names = parse_object_names(response.content)
        self._canonical = {name.lower(): name for name in names}
        return [name.lower() for name in names]

    def _canonical_name(self, name: str) -> str:
        if not self._canonical:
            self.list_objects()
        return self._canonical.get(name.lower(), name)

    def _describe(self, name: str) -> Tuple[Dict[str, str], Dict[str, str]]:
        key = name.lower()
        if key in self._describe_cache:
            return self._describe_cache[key]
        canonical = self._canonical_name(name)
        response = self._http.request(
            "GET", f"{self._url_base}/v1/describe/{canonical}", headers=self._http.headers()
        )
        fields = parse_fields(response.content)
        foreign_keys = foreign_key_columns(fields, parse_relationships(response.content))
        self._describe_cache[key] = (fields, foreign_keys)
        return self._describe_cache[key]

    def describe_object(self, name: str) -> Mapping[str, Mapping[str, Any]]:
        fields, foreign_keys = self._describe(name)
        if not fields:
            return {}
        schema: Dict[str, Mapping[str, Any]] = {
            field.lower(): {"type": json_type(zuora_type)}
            for field, zuora_type in fields.items()
        }
        for column in foreign_keys.values():
            schema.setdefault(column, {"type": TYPE_STRING})
        return schema

    def warm_describe_cache(self, names: List[str]) -> None:
        """
        Pre-populate schemas concurrently. Each object needs its own Describe call,
        and doing them serially on a large tenant exceeds Airbyte's discover timeout.
        """
        if not self._canonical:
            self.list_objects()
        pending = [name for name in names if name.lower() not in self._describe_cache]
        if not pending:
            return
        if self._describe_concurrency <= 1 or len(pending) == 1:
            for name in pending:
                self._describe(name)
            return
        pool = ThreadPoolExecutor(max_workers=self._describe_concurrency)
        try:
            futures = [pool.submit(self._describe, name) for name in pending]
            for future in futures:
                future.result()  # re-raise the first failure, if any
        finally:
            # On a failure, drop not-yet-started describes rather than running them
            # all before the error surfaces (fail fast).
            pool.shutdown(wait=True, cancel_futures=True)

    # --- job lifecycle ---

    def submit_job(self, query: str, name: str = "airbyte") -> str:
        body = {
            "format": "csv",
            "version": "1.0",
            "encrypted": "none",
            # Without this Zuora returns human labels ("Account: Account Number")
            # instead of API names ("Account.AccountNumber"). The flag reads
            # backwards from its name; `true` is the one we want.
            "useQueryLabels": True,
            "queries": [{"name": name, "query": query, "type": "zoqlexport"}],
        }
        response = self._http.request(
            "POST",
            f"{self._url_base}/v1/batch-query/",
            headers=self._http.headers(),
            json=body,
        )
        data = response.json()
        # A rejected query comes back HTTP 200 with no job id, so the body decides.
        job_id = data.get("id")
        if not job_id or data.get("status") == "error" or data.get("success") is False:
            message = data.get("message") or _first_reason(data) or "AQuA rejected the query"
            raise ZOQLQueryFailed(message, query)
        return job_id

    def poll_job(self, job_id: str, query: str = "") -> str:
        attempts = 0
        while True:
            attempts += 1
            if attempts > self._max_poll_attempts:
                raise ZOQLQueryFailed(
                    f"Polling timed out after {self._max_poll_attempts} attempts", query
                )
            response = self._http.request(
                "GET",
                f"{self._url_base}/v1/batch-query/jobs/{job_id}",
                headers=self._http.headers(),
            )
            data = response.json()
            status = (data.get("status") or "").lower()
            batches = data.get("batches") or []
            if status == _DONE_STATUS:
                file_id = batches[0].get("fileId") if batches else None
                if not file_id:
                    raise ZOQLQueryFailed("AQuA job completed with no result file", query)
                return file_id
            if status in _ERROR_STATUSES:
                message = _job_message(data, batches)
                if _NO_SELECT_MARKER in message.lower():
                    raise ZOQLQueryCannotProcessObject(message)
                if is_transient_job_error(message):
                    raise ZuoraTransientError(f"AQuA job failed transiently: {message}")
                raise ZOQLQueryFailed(message, query)
            time.sleep(self._poll_interval)

    def _download(self, file_id: str) -> Iterator[Mapping[str, Optional[str]]]:
        response = self._http.request(
            "GET",
            f"{self._url_base}/v1/file/{file_id}",
            headers=self._http.headers(),
            stream=True,
        )
        # `response.raw` bypasses requests' content decoding, and csv must see the
        # whole stream (not iter_lines, which splits inside quoted fields).
        response.raw.decode_content = True
        yield from csv.DictReader(io.TextIOWrapper(response.raw, encoding="utf-8", newline=""))

    # --- reading ---

    def read_object(
        self,
        name: str,
        cursor: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> Iterator[Mapping[str, Any]]:
        canonical = self._canonical_name(name)
        fields, foreign_keys = self._describe(name)
        types = {key: list(value["type"]) for key, value in self.describe_object(name).items()}
        query = render_query(
            canonical,
            foreign_keys.keys(),
            cursor=_canonical_field(fields, cursor),
            start=start,
            end=end,
        )
        # Retry the submit -> poll cycle on transient failures. Only the pre-download
        # phase is retried, so no partially-yielded records are ever duplicated.
        for attempt in range(self._max_job_retries + 1):
            try:
                file_id = self.poll_job(self.submit_job(query, name=name), query)
            except ZuoraTransientError:
                if attempt >= self._max_job_retries:
                    raise
                self._http.sleep_before_retry(attempt, None)
                continue
            for row in self._download(file_id):
                yield normalize_row(canonical, row, types)
            return
