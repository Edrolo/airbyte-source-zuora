# Zuora CDK 7.x Modernization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `source-zuora` to `airbyte-cdk` 7.x with per-stream incremental state, Poetry packaging, and current Airbyte connector conventions.

**Architecture:** Extract the ZOQL submit→poll→download job flow into a plain `ZuoraQueryClient` (no CDK HTTP internals). Each Zuora object becomes a `ZuoraObjectStream(Stream, IncrementalMixin)` that owns per-stream state and drives the client. `SourceZuora(AbstractSource)` discovers objects and builds one stream per object.

**Tech Stack:** Python 3.10–3.13, `airbyte-cdk ^7.23`, `pendulum`, Poetry, `pytest` + `requests-mock` + `pytest-mock`.

## Global Constraints

- `airbyte-cdk` requires **Python `>=3.10,<3.14`**. Local default is 3.14.6 (unusable); use the uv-managed 3.13 interpreter at `/Users/johndagostino/.local/share/uv/python/cpython-3.13.3-macos-aarch64-none/bin/python3.13`.
- Poetry is the build tool. Install via `uv tool install poetry`. Pin the env with `poetry env use <3.13 path>`.
- No `AirbyteLogger` (removed) — use `logging.getLogger("airbyte")`.
- No `sys.exit()` in library code. Config errors raise `AirbyteTracedException(failure_type=FailureType.config_error)`.
- Verified import paths (airbyte-cdk 7.23.6):
  - `from airbyte_cdk.sources import AbstractSource`
  - `from airbyte_cdk.sources.streams import Stream, IncrementalMixin`
  - `from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator`
  - `from airbyte_cdk.models import SyncMode, FailureType`
  - `from airbyte_cdk.utils import AirbyteTracedException`
  - `from airbyte_cdk.entrypoint import launch`
- `Stream.stream_slices` is keyword-only: `(self, *, sync_mode, cursor_field=None, stream_state=None)`.
- All shell commands run from repo root `/Users/johndagostino/Code/airbyte-source-zuora` and use `poetry run` for Python.

---

### Task 1: Poetry packaging & environment

**Files:**
- Modify/replace: `pyproject.toml`
- Delete: `setup.py`, `setup.cfg`, `main.py`, `PKG-INFO`, `airbyte_source_zuora.egg-info/` (whole dir)
- Create: (generated) `poetry.lock`

**Interfaces:**
- Produces: a working Poetry env on Python 3.13 with `airbyte-cdk` importable; console script `source-zuora`.

- [ ] **Step 1: Replace `pyproject.toml`**

```toml
[tool.poetry]
name = "airbyte-source-zuora"
version = "0.2.0"
description = "Airbyte source connector for Zuora."
authors = ["Airbyte <contact@airbyte.io>"]
license = "MIT"
readme = "README.md"
packages = [{ include = "source_zuora" }]

[tool.poetry.dependencies]
python = ">=3.10,<3.14"
airbyte-cdk = "^7.23"
pendulum = "*"

[tool.poetry.group.dev.dependencies]
pytest = "*"
requests-mock = "*"
pytest-mock = "*"

[tool.poetry.scripts]
source-zuora = "source_zuora.run:run"

[build-system]
requires = ["poetry-core>=1.0.0"]
build-backend = "poetry.core.masonry.api"
```

- [ ] **Step 2: Install Poetry and pin the interpreter**

```bash
uv tool install poetry
poetry env use /Users/johndagostino/.local/share/uv/python/cpython-3.13.3-macos-aarch64-none/bin/python3.13
```
Expected: Poetry reports "Using virtualenv" on Python 3.13.3.

- [ ] **Step 3: Delete legacy packaging files**

```bash
git rm -r --cached setup.py setup.cfg PKG-INFO main.py airbyte_source_zuora.egg-info
rm -rf setup.py setup.cfg PKG-INFO main.py airbyte_source_zuora.egg-info
```

- [ ] **Step 4: Install dependencies (generates lockfile)**

```bash
poetry install
```
Expected: resolves and installs `airbyte-cdk` 7.23.x; creates `poetry.lock`.

- [ ] **Step 5: Verify the env**

```bash
poetry run python -c "import airbyte_cdk; print(airbyte_cdk.__version__)"
```
Expected: prints `7.23.x`.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml poetry.lock
git commit -m "build: migrate source-zuora packaging to Poetry + airbyte-cdk 7.x"
```

---

### Task 2: Modernize errors & logging (`zuora_errors.py`)

**Files:**
- Modify: `source_zuora/zuora_errors.py`
- Test: `unit_tests/test_errors.py`

**Interfaces:**
- Produces:
  - `QueryWindowError(value)` — subclass of `AirbyteTracedException`, `failure_type=config_error`.
  - `ZOQLQueryFailed(message: str, query: str = "")` — `Exception`, attrs `.message`, `.query`.
  - `ZOQLQueryCannotProcessObject(message: str = "")` — `Exception`, attr `.message`.

- [ ] **Step 1: Write the failing test**

```python
# unit_tests/test_errors.py
import pytest
from airbyte_cdk.models import FailureType
from airbyte_cdk.utils import AirbyteTracedException
from source_zuora.zuora_errors import (
    QueryWindowError,
    ZOQLQueryFailed,
    ZOQLQueryCannotProcessObject,
)


def test_query_window_error_is_config_traced_exception():
    err = QueryWindowError("abc")
    assert isinstance(err, AirbyteTracedException)
    assert err.failure_type == FailureType.config_error
    assert "abc" in err.message


def test_zoql_query_failed_carries_message_and_query():
    err = ZOQLQueryFailed("boom", "select * from account")
    assert err.message == "boom"
    assert err.query == "select * from account"
    assert isinstance(err, Exception)


def test_cannot_process_object_carries_message():
    err = ZOQLQueryCannotProcessObject("nope")
    assert err.message == "nope"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest unit_tests/test_errors.py -v`
Expected: FAIL (ImportError / attribute mismatch — old module still has `AirbyteLogger` classes).

- [ ] **Step 3: Replace `zuora_errors.py`**

```python
#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from typing import Any

from airbyte_cdk.models import FailureType
from airbyte_cdk.utils import AirbyteTracedException


class QueryWindowError(AirbyteTracedException):
    """Raised when the `window_in_days` config value is not numeric."""

    def __init__(self, value: Any):
        message = (
            f"`Query Window` is set to '{value}', please make sure you use a "
            f"float or integer, not a string."
        )
        super().__init__(
            message=message,
            internal_message=message,
            failure_type=FailureType.config_error,
        )


class ZOQLQueryFailed(Exception):
    """A ZOQL Data Query job terminated in a non-completed state."""

    def __init__(self, message: str, query: str = ""):
        self.message = message
        self.query = query
        super().__init__(f"{message}, QUERY: {query}")


class ZOQLQueryCannotProcessObject(Exception):
    """
    A Zuora object cannot be read (permissions / subscription plan / API user
    permissions). Non-critical: the stream is skipped and the sync continues.
    """

    def __init__(
        self,
        message: str = (
            "The stream cannot be processed, check the Zuora object's permissions / "
            "subscription plan / API user permissions. This warning is not critical "
            "and can be ignored."
        ),
    ):
        self.message = message
        super().__init__(message)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest unit_tests/test_errors.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add source_zuora/zuora_errors.py unit_tests/test_errors.py
git commit -m "refactor: modernize zuora error taxonomy (AirbyteTracedException, no sys.exit)"
```

---

### Task 3: Modernize authenticator (`zuora_auth.py`)

**Files:**
- Modify: `source_zuora/zuora_auth.py`
- Test: `unit_tests/test_auth.py`

**Interfaces:**
- Consumes: `get_url_base` from `zuora_endpoint`.
- Produces:
  - `ZuoraAuthenticator(config).url_base -> str`
  - `ZuoraAuthenticator(config).get_auth() -> ZuoraOauth2Authenticator`
  - `ZuoraOauth2Authenticator.build_refresh_request_body()` excludes `refresh_token`, sets `grant_type=client_credentials`.

- [ ] **Step 1: Write the failing test**

```python
# unit_tests/test_auth.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest unit_tests/test_auth.py -v`
Expected: FAIL (old import path `...oauth` still in use / `build_refresh_request_body` includes `refresh_token`).

- [ ] **Step 3: Replace `zuora_auth.py`**

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest unit_tests/test_auth.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add source_zuora/zuora_auth.py unit_tests/test_auth.py
git commit -m "refactor: modernize Zuora OAuth2 authenticator for airbyte-cdk 7.x"
```

---

### Task 4: Extract `ZuoraQueryClient` (`zuora_client.py`)

**Files:**
- Create: `source_zuora/zuora_client.py`
- Test: `unit_tests/test_client.py`

**Interfaces:**
- Consumes: `ZOQLQueryFailed`, `ZOQLQueryCannotProcessObject` from `zuora_errors`.
- Produces:
  - `ZuoraQueryClient(url_base, authenticator, data_query="Live", poll_interval=1.0, session=None)`
  - `.run_query(zoql: str) -> Iterator[Mapping[str, Any]]`
  - `.list_objects() -> list[str]`
  - `.describe_object(name: str) -> dict[str, dict]` (JSON-schema `properties`)
  - Module constant `TYPE_MAPPING: dict[str, list[str]]`

- [ ] **Step 1: Write the failing tests**

```python
# unit_tests/test_client.py
import json
import pytest
from source_zuora.zuora_client import ZuoraQueryClient
from source_zuora.zuora_errors import ZOQLQueryFailed, ZOQLQueryCannotProcessObject


class FakeAuth:
    def get_auth_header(self):
        return {"Authorization": "Bearer test"}


BASE = "https://rest.zuora.com"


def make_client():
    return ZuoraQueryClient(BASE, FakeAuth(), poll_interval=0)


def register_job(requests_mock, statuses, data_file="https://s3/result.jsonl", error=""):
    # submit
    requests_mock.post(f"{BASE}/query/jobs", json={"data": {"id": "job-1"}})
    # poll: one response per status in `statuses`
    responses = []
    for s in statuses:
        d = {"queryStatus": s, "query": "select 1"}
        if s == "completed":
            d["dataFile"] = data_file
        if error:
            d["errorMessage"] = error
        responses.append({"json": {"data": d}})
    requests_mock.get(f"{BASE}/query/jobs/job-1", responses)


def test_run_query_happy_path(requests_mock):
    register_job(requests_mock, ["in_progress", "completed"])
    requests_mock.get(
        "https://s3/result.jsonl",
        text='{"id": "a"}\n{"id": "b"}\n',
    )
    client = make_client()
    assert list(client.run_query("select * from account")) == [{"id": "a"}, {"id": "b"}]


def test_run_query_failed_raises(requests_mock):
    register_job(requests_mock, ["failed"], error="something exploded")
    with pytest.raises(ZOQLQueryFailed) as exc:
        list(make_client().run_query("select * from account"))
    assert exc.value.message == "something exploded"


def test_run_query_process_object_raises_skippable(requests_mock):
    register_job(requests_mock, ["failed"], error="failed to process object")
    with pytest.raises(ZOQLQueryCannotProcessObject):
        list(make_client().run_query("select * from account"))


def test_list_objects(requests_mock):
    register_job(requests_mock, ["completed"])
    requests_mock.get("https://s3/result.jsonl", text='{"Table": "account"}\n{"Table": "user"}\n')
    assert make_client().list_objects() == ["account", "user"]


def test_describe_object_maps_types(requests_mock):
    register_job(requests_mock, ["completed"])
    requests_mock.get(
        "https://s3/result.jsonl",
        text='{"Column": "id", "Type": "varchar"}\n{"Column": "balance", "Type": "decimal"}\n',
    )
    props = make_client().describe_object("account")
    assert props["id"] == {"type": ["string", "null"]}
    assert props["balance"] == {"type": ["number", "null"]}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest unit_tests/test_client.py -v`
Expected: FAIL (`ModuleNotFoundError: source_zuora.zuora_client`).

- [ ] **Step 3: Create `zuora_client.py`**

```python
#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import json
import logging
import time
from typing import Any, Iterator, List, Mapping, MutableMapping, Optional

import requests

from .zuora_errors import ZOQLQueryCannotProcessObject, ZOQLQueryFailed

logger = logging.getLogger("airbyte")

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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest unit_tests/test_client.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add source_zuora/zuora_client.py unit_tests/test_client.py
git commit -m "feat: add ZuoraQueryClient for ZOQL submit/poll/download flow"
```

---

### Task 5: Stream & Source (`source.py`)

**Files:**
- Replace: `source_zuora/source.py`
- Test: `unit_tests/test_source.py`

**Interfaces:**
- Consumes: `ZuoraQueryClient`, `ZuoraAuthenticator`, `ZUORA_EXCLUDED_STREAMS`, `QueryWindowError`, `ZOQLQueryFailed`, `ZOQLQueryCannotProcessObject`.
- Produces:
  - `ZuoraObjectStream(name, client, config)` — `Stream, IncrementalMixin`; `primary_key="id"`; `cursor_field` cached property (`"updateddate"` | `"createddate"` | `[]`); `state` get/set; `read_records(...)`; `stream_slices(*, sync_mode, ...)`.
  - `SourceZuora(AbstractSource)` — `check_connection(logger, config)`, `streams(config)`.

- [ ] **Step 1: Write the failing tests**

```python
# unit_tests/test_source.py
import pendulum
import pytest
from airbyte_cdk.models import SyncMode
from source_zuora.source import ZuoraObjectStream, SourceZuora


CONFIG = {
    "start_date": "2021-01-01",
    "window_in_days": "10",
    "data_query": "Live",
    "tenant_endpoint": "US Production",
    "client_id": "cid",
    "client_secret": "secret",
}


class FakeClient:
    def __init__(self, schema, rows=None, errors=None):
        self._schema = schema
        self._rows = rows or []
        self._errors = errors or []
        self.queries = []

    def describe_object(self, name):
        return self._schema

    def run_query(self, zoql):
        self.queries.append(zoql)
        if self._errors:
            raise self._errors.pop(0)
        yield from self._rows


def make_stream(client):
    return ZuoraObjectStream("account", client, CONFIG)


def test_cursor_field_prefers_updateddate():
    client = FakeClient({"updateddate": {"type": ["string", "null"]}, "createddate": {}})
    assert make_stream(client).cursor_field == "updateddate"


def test_cursor_field_falls_back_to_createddate():
    client = FakeClient({"createddate": {"type": ["string", "null"]}})
    assert make_stream(client).cursor_field == "createddate"


def test_cursor_field_empty_when_no_cursor():
    client = FakeClient({"name": {"type": ["string", "null"]}})
    assert make_stream(client).cursor_field == []


def test_state_round_trip():
    client = FakeClient({"updateddate": {}})
    stream = make_stream(client)
    stream.state = {"updateddate": "2021-05-05 00:00:00.000000 "}
    assert stream.state == {"updateddate": "2021-05-05 00:00:00.000000 "}


def test_stream_slices_windows_from_start_date():
    client = FakeClient({"updateddate": {}})
    stream = make_stream(client)
    slices = list(stream.stream_slices(sync_mode=SyncMode.incremental))
    assert slices  # at least one window
    assert set(slices[0].keys()) == {"start_date", "end_date"}


def test_read_records_advances_cursor():
    rows = [{"id": "1", "updateddate": "2021-01-02"}, {"id": "2", "updateddate": "2021-01-03"}]
    client = FakeClient({"updateddate": {}}, rows=rows)
    stream = make_stream(client)
    out = list(
        stream.read_records(
            sync_mode=SyncMode.incremental,
            stream_slice={"start_date": "2021-01-01 00:00:00.000000 ", "end_date": "2021-01-10 00:00:00.000000 "},
        )
    )
    assert out == rows
    assert stream.state == {"updateddate": "2021-01-03"}


def test_read_records_skips_unprocessable_object():
    from source_zuora.zuora_errors import ZOQLQueryCannotProcessObject
    client = FakeClient({"updateddate": {}}, errors=[ZOQLQueryCannotProcessObject()])
    stream = make_stream(client)
    out = list(
        stream.read_records(
            sync_mode=SyncMode.incremental,
            stream_slice={"start_date": "2021-01-01 00:00:00.000000 ", "end_date": "2021-01-10 00:00:00.000000 "},
        )
    )
    assert out == []


def test_read_records_falls_back_to_full_on_cursor_error():
    from source_zuora.zuora_errors import ZOQLQueryFailed
    rows = [{"id": "1"}]
    client = FakeClient(
        {"updateddate": {}},
        rows=rows,
        errors=[ZOQLQueryFailed("Column 'updateddate' cannot be resolved", "q")],
    )
    stream = make_stream(client)
    out = list(
        stream.read_records(
            sync_mode=SyncMode.incremental,
            stream_slice={"start_date": "2021-01-01 00:00:00.000000 ", "end_date": "2021-01-10 00:00:00.000000 "},
        )
    )
    assert out == rows
    # second query was the full-object fetch
    assert client.queries[-1].strip().lower().startswith("select * from account")


def test_check_connection_invalid_tenant_returns_false():
    ok, msg = SourceZuora().check_connection(
        logger=None, config={**CONFIG, "tenant_endpoint": "Nonexistent"}
    )
    assert ok is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest unit_tests/test_source.py -v`
Expected: FAIL (old `source.py` imports `AirbyteLogger` / has no `ZuoraObjectStream`).

- [ ] **Step 3: Replace `source.py`**

```python
#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import logging
from datetime import datetime
from functools import cached_property
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import pendulum
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import IncrementalMixin, Stream

from .zuora_auth import ZuoraAuthenticator
from .zuora_client import ZuoraQueryClient
from .zuora_errors import (
    QueryWindowError,
    ZOQLQueryCannotProcessObject,
    ZOQLQueryFailed,
)
from .zuora_excluded_streams import ZUORA_EXCLUDED_STREAMS

logger = logging.getLogger("airbyte")

CURSOR_FIELD = "updateddate"
ALT_CURSOR_FIELD = "createddate"


class ZuoraObjectStream(Stream, IncrementalMixin):
    """
    One dynamically-discovered Zuora object. Emits per-stream incremental state
    keyed on the resolved cursor field.
    """

    primary_key = "id"

    def __init__(self, name: str, client: ZuoraQueryClient, config: Mapping[str, Any]):
        self._name = name
        self._client = client
        self._config = config
        self._cursor_value: Optional[str] = None

    @property
    def name(self) -> str:
        return self._name

    @cached_property
    def cursor_field(self):
        properties = self.get_json_schema()["properties"]
        if CURSOR_FIELD in properties:
            return CURSOR_FIELD
        if ALT_CURSOR_FIELD in properties:
            return ALT_CURSOR_FIELD
        return []

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value and self.cursor_field:
            return {self.cursor_field: self._cursor_value}
        return {}

    @state.setter
    def state(self, value: Mapping[str, Any]) -> None:
        self._cursor_value = value.get(CURSOR_FIELD) or value.get(ALT_CURSOR_FIELD)

    @property
    def state_checkpoint_interval(self) -> Optional[int]:
        return None  # records within a slice are not guaranteed globally ordered

    @property
    def window_in_days(self) -> float:
        value = self._config.get("window_in_days", "90")
        try:
            return float(value)
        except (TypeError, ValueError):
            raise QueryWindowError(value)

    def get_json_schema(self) -> Mapping[str, Any]:
        return {"type": "object", "properties": dict(self._client.describe_object(self.name))}

    @staticmethod
    def _to_datetime_str(date: datetime) -> str:
        # e.g. '2021-07-15 07:45:55.000000 -07:00' — format Zuora accepts as TIMESTAMP
        return date.strftime("%Y-%m-%d %H:%M:%S.%f %Z")

    def stream_slices(
        self, *, sync_mode=SyncMode.full_refresh, cursor_field=None, stream_state=None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        start_date = pendulum.parse(self._config["start_date"]).astimezone()
        end_date = pendulum.now().astimezone()

        cursor_state = self.state.get(self.cursor_field) if self.cursor_field else None
        if cursor_state:
            start_date = pendulum.parse(cursor_state)

        start_date = min(start_date, end_date)
        while start_date <= end_date:
            end_slice = start_date.add(days=self.window_in_days)
            yield {
                "start_date": self._to_datetime_str(start_date),
                "end_date": self._to_datetime_str(end_slice),
            }
            start_date = end_slice

    def _query_incremental(self, cursor: str, stream_slice: Mapping[str, Any]) -> str:
        return (
            f"select * from {self.name} where "
            f"{cursor} >= TIMESTAMP '{stream_slice.get('start_date')}' and "
            f"{cursor} <= TIMESTAMP '{stream_slice.get('end_date')}' "
            f"order by {cursor} asc"
        )

    def _query_full(self) -> str:
        return f"select * from {self.name}"

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        cursor = self.cursor_field
        try:
            if cursor and stream_slice:
                query = self._query_incremental(cursor, stream_slice)
            else:
                query = self._query_full()
            for record in self._client.run_query(query):
                if cursor:
                    incoming = record.get(cursor)
                    if incoming:
                        self._cursor_value = max(self._cursor_value or "", incoming)
                yield record
        except ZOQLQueryCannotProcessObject:
            # non-critical: skip this stream
            return
        except ZOQLQueryFailed as error:
            if "cannot be resolved" not in (error.message or ""):
                raise
            # schema advertised a cursor the query engine rejected — fetch full object
            yield from self._client.run_query(self._query_full())


class SourceZuora(AbstractSource):
    def check_connection(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> Tuple[bool, Optional[Any]]:
        try:
            auth = ZuoraAuthenticator(config)
            if not auth.url_base:
                return False, f"Unknown tenant_endpoint: {config.get('tenant_endpoint')!r}"
            auth.get_auth().get_auth_header()
            return True, None
        except Exception as error:
            return False, str(error)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = ZuoraAuthenticator(config)
        client = ZuoraQueryClient(
            url_base=auth.url_base,
            authenticator=auth.get_auth(),
            data_query=config.get("data_query", "Live"),
        )
        return [
            ZuoraObjectStream(name, client, config)
            for name in client.list_objects()
            if name not in ZUORA_EXCLUDED_STREAMS
        ]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest unit_tests/test_source.py -v`
Expected: PASS (10 tests).

- [ ] **Step 5: Run the full unit suite**

Run: `poetry run pytest unit_tests/ -v`
Expected: PASS (all tasks' tests).

- [ ] **Step 6: Commit**

```bash
git add source_zuora/source.py unit_tests/test_source.py
git commit -m "feat: per-stream incremental streams + AbstractSource for airbyte-cdk 7.x"
```

---

### Task 6: Airbyte metadata & acceptance-test config

**Files:**
- Create: `metadata.yaml`
- Create: `acceptance-test-config.yml`
- Create: `unit_tests/__init__.py` (empty, if not already created by earlier tasks)

**Interfaces:**
- Produces: Airbyte-standard connector metadata (base-image build) and CAT config.

- [ ] **Step 1: Create `metadata.yaml`**

```yaml
data:
  allowedHosts:
    hosts:
      - "${tenant_endpoint}"
      - "*.zuora.com"
      - "*.amazonaws.com"
  connectorSubtype: api
  connectorType: source
  definitionId: 3dc3037c-5ce8-4661-adc2-f7a9e3c5ece5  # real id from the Airbyte registry
  dockerImageTag: 0.2.0  # revival bump; upstream was archived at 0.1.3
  dockerRepository: airbyte/source-zuora
  githubIssueLabel: source-zuora
  icon: zuora.svg
  license: MIT
  name: Zuora
  releaseStage: alpha
  supportLevel: community
  documentationUrl: https://docs.airbyte.com/integrations/sources/zuora
  tags:
    - language:python
    - cdk:python
  connectorBuildOptions:
    baseImage: docker.io/airbyte/python-connector-base:2.0.0@sha256:c44839ba84406116e8ba68722a0f30e8f6e7056c726f447681bb9e9ece8bd916
  ab_internal:
    ql: 100
    sl: 100
  registryOverrides:
    oss:
      enabled: true
    cloud:
      enabled: false
metadataSpecVersion: "1.0"
```

> NOTE: `definitionId` and the `baseImage` sha must match the values in Airbyte's connector registry for the real published connector. Before merging, replace `definitionId` with the connector's actual UUID (from the current `metadata.yaml` in the airbyte monorepo) and pin `baseImage` to the latest `python-connector-base` digest. This file is a scaffold for local `airbyte-ci` use.

- [ ] **Step 2: Create `acceptance-test-config.yml`**

```yaml
connector_image: airbyte/source-zuora:dev
acceptance_tests:
  spec:
    tests:
      - spec_path: "source_zuora/spec.json"
  connection:
    tests:
      - config_path: "secrets/config.json"
        status: "succeed"
      - config_path: "integration_tests/invalid_config.json"
        status: "failed"
  discovery:
    tests:
      - config_path: "secrets/config.json"
  basic_read:
    tests:
      - config_path: "secrets/config.json"
        configured_catalog_path: "integration_tests/configured_catalog.json"
        empty_streams: []
  incremental:
    tests:
      - config_path: "secrets/config.json"
        configured_catalog_path: "integration_tests/configured_catalog.json"
        future_state:
          future_state_path: "integration_tests/abnormal_state.json"
  full_refresh:
    tests:
      - config_path: "secrets/config.json"
        configured_catalog_path: "integration_tests/configured_catalog.json"
```

- [ ] **Step 3: Ensure `unit_tests` is a package**

```bash
touch unit_tests/__init__.py
```

- [ ] **Step 4: Commit**

```bash
git add metadata.yaml acceptance-test-config.yml unit_tests/__init__.py
git commit -m "chore: add Airbyte metadata.yaml and acceptance-test-config.yml"
```

---

### Task 7: End-to-end verification & docs

**Files:**
- Modify: `README.md` (local-dev commands → Poetry)
- Verify only: no code changes.

**Interfaces:** none (verification task).

- [ ] **Step 1: `spec` command works**

Run: `poetry run source-zuora spec`
Expected: emits an `AirbyteMessage` of type `SPEC` containing the `connectionSpecification` from `spec.json` (JSON on stdout, no traceback).

- [ ] **Step 2: `check` fails gracefully on invalid config (no live creds)**

Run: `poetry run source-zuora check --config integration_tests/invalid_config.json`
Expected: emits a `CONNECTION_STATUS` message with `status: FAILED` (not a Python traceback).

- [ ] **Step 3: Full unit suite green**

Run: `poetry run pytest unit_tests/ -v`
Expected: PASS (all tests from Tasks 2–5).

- [ ] **Step 4: Update README local-dev section**

Replace the `pip install -r requirements.txt` / `python main.py ...` instructions with:

```markdown
### Local development

This connector uses [Poetry](https://python-poetry.org/) and requires Python 3.10–3.13
(airbyte-cdk does not yet support 3.14).

```bash
poetry env use python3.13
poetry install
poetry run source-zuora spec
poetry run source-zuora check --config secrets/config.json
poetry run source-zuora discover --config secrets/config.json
poetry run source-zuora read --config secrets/config.json --catalog integration_tests/configured_catalog.json
poetry run pytest unit_tests/
```
```

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs: update local-dev instructions for Poetry + airbyte-cdk 7.x"
```

---

## Verification gap (stated explicitly)

- `check`, `discover`, and `read` against **live Zuora** require OAuth credentials in
  `secrets/config.json`, which are not available in this environment. Those paths are
  covered by unit tests (mocked HTTP) and by Connector Acceptance Tests in CI once
  credentials are provided. Local verification is limited to `spec`, graceful
  `check` failure, and the unit suite.

## Self-review notes

- **Spec coverage:** packaging (T1), logging/errors (T2), auth (T3), client extraction (T4), per-stream state + streams + source (T5), metadata/CAT (T6), verification/docs (T7). All design sections mapped.
- **Design deltas (intentional):** (a) cursor "cannot be resolved" fallback simplified to a single full-object fetch since `cursor_field` already resolves from schema; (b) `as_airbyte_stream` not overridden — CDK derives sync modes from `cursor_field`; (c) dynamic `type(...)` class creation dropped in favor of an instance-level `name` — simpler and testable. All preserve behavior.
- **Type consistency:** `run_query`/`list_objects`/`describe_object`, `cursor_field`, `state`, and `ZOQLQueryFailed(message, query)` names are consistent across tasks.
