# Zuora AQuA Query Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `query_api` config flag that selects between the existing Data Query backend and a new AQuA (Export ZOQL) backend, behind one `QueryBackend` interface.

**Architecture:** Query construction moves out of `source.py` into the backend, because the two APIs use different dialects, object names, and transports. `source.py` keeps stream slicing and cursor state and calls `read_object(name, cursor, start, end)` with `datetime` bounds; each backend formats those bounds in its own dialect. Shared HTTP retry logic and the Zuora-type-to-JSON-type map are extracted so both backends share them without importing each other.

**Tech Stack:** Python 3.10–3.13, `airbyte-cdk ^7.23`, `requests`, `pendulum`, stdlib `csv` and `xml.etree.ElementTree` (no new dependencies), `pytest` + `requests-mock`.

**Spec:** `docs/superpowers/specs/2026-08-20-zuora-aqua-backend-design.md`

## Global Constraints

- No new runtime dependencies. XML via stdlib `xml.etree.ElementTree`, CSV via stdlib `csv`.
- Data Query remains the default. `query_api` is optional and absent from `spec.json`'s `required` array, so existing saved configs stay valid.
- Every AQuA submit body sets `useQueryLabels: true`. Without it Zuora returns human labels (`Account: Account Number`) instead of API names (`Account.AccountNumber`). Spec F2.
- AQuA datetime literals are ISO-8601 with an explicit offset, produced by `datetime.isoformat()`, e.g. `'2026-08-01T00:00:00+10:00'`. A space-separated literal is accepted by Zuora and **silently returns the entire table**. Spec F3.
- AQuA relationship names come only from the `<related-objects>` section of the describe XML, never guessed. An unknown name is a submit-time error. Spec F10.
- AQuA output format is `csv`. `format: "json"` is rejected at submit. Spec F1.
- CSV parsing must go through `csv.DictReader` over a `TextIOWrapper` on the raw stream, never `iter_lines()`, which corrupts quoted fields containing newlines.
- Existing behaviour of the Data Query path must not change. `unit_tests/test_client.py` passes untouched except where a task explicitly says otherwise.
- Run tests with `poetry run pytest`.

---

## File Structure

| File | Responsibility |
|---|---|
| `source_zuora/zuora_types.py` | **New.** Zuora-type to JSON-schema-type map and `json_type()` lookup. Shared by both backends; separate module so neither backend imports the other. |
| `source_zuora/zuora_http.py` | **New.** `ZuoraHttpClient`: auth headers, retry/backoff, HTTP error classification. Extracted verbatim from `zuora_client.py`. |
| `source_zuora/zuora_describe.py` | **New.** Pure functions parsing AQuA describe XML. No HTTP, no state. |
| `source_zuora/zuora_backend.py` | **New.** `QueryBackend` ABC and `get_backend(config)` factory. |
| `source_zuora/zuora_aqua_client.py` | **New.** `ZuoraAquaClient`: AQuA job lifecycle, query rendering, CSV normalization. |
| `source_zuora/zuora_client.py` | Modified. Implements `QueryBackend`, gains `read_object`, delegates HTTP to `ZuoraHttpClient`, imports types from `zuora_types`. |
| `source_zuora/zuora_errors.py` | Modified. Gains `is_transient_job_error()`, shared job-error classification. |
| `source_zuora/source.py` | Modified. Uses `get_backend()`; query builders removed; slices carry `datetime` not `str`. |
| `source_zuora/spec.json` | Modified. New optional `query_api` property. |

Note: the spec put the `TYPE_MAPPING` fix in `zuora_client.py`. It moves to `zuora_types.py` instead so `zuora_aqua_client.py` can use it without importing the Data Query backend. Same fix, better home.

---

### Task 1: Shared type map, with the `ZOQL`/`number` fix

Spec F6. `TYPE_MAPPING` currently has lowercase `zoql` (Zuora sends `ZOQL`) and no `number` key, so both silently degrade to string on the Data Query path too.

**Files:**
- Create: `source_zuora/zuora_types.py`
- Modify: `source_zuora/zuora_client.py:19-51` (remove the type constants and map, import them instead)
- Test: `unit_tests/test_types.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `TYPE_NUMBER`, `TYPE_STRING`, `TYPE_OBJECT`, `TYPE_ARRAY`, `TYPE_BOOL` (each `List[str]`), `TYPE_MAPPING: Dict[str, List[str]]`, and `json_type(zuora_type: Optional[str]) -> List[str]`.

- [ ] **Step 1: Write the failing test**

Create `unit_tests/test_types.py`:

```python
from source_zuora.zuora_types import TYPE_BOOL, TYPE_NUMBER, TYPE_STRING, json_type


def test_json_type_is_case_insensitive():
    # Zuora's Describe API returns "ZOQL" uppercase; Data Query returns lowercase.
    assert json_type("ZOQL") == json_type("zoql")


def test_json_type_maps_number():
    # "number" appears in Describe output and was missing from the map entirely.
    assert json_type("number") == TYPE_NUMBER


def test_json_type_known_scalars():
    assert json_type("decimal") == TYPE_NUMBER
    assert json_type("integer") == TYPE_NUMBER
    assert json_type("boolean") == TYPE_BOOL
    assert json_type("datetime") == TYPE_STRING
    assert json_type("picklist") == TYPE_STRING


def test_json_type_unknown_and_empty_default_to_string():
    assert json_type("wat") == TYPE_STRING
    assert json_type("") == TYPE_STRING
    assert json_type(None) == TYPE_STRING


def test_json_type_strips_whitespace():
    assert json_type("  decimal  ") == TYPE_NUMBER
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest unit_tests/test_types.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'source_zuora.zuora_types'`

- [ ] **Step 3: Write minimal implementation**

Create `source_zuora/zuora_types.py`:

```python
#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from typing import Dict, List, Optional

TYPE_NUMBER = ["number", "null"]
TYPE_STRING = ["string", "null"]
TYPE_OBJECT = ["object", "null"]
TYPE_ARRAY = ["array", "null"]
TYPE_BOOL = ["boolean", "null"]

# Keys are lowercase; look them up through json_type(), which casefolds. The Data
# Query `DESCRIBE` output and the AQuA Describe API disagree on case (the latter
# returns e.g. "ZOQL"), so a case-sensitive lookup silently degrades types to string.
TYPE_MAPPING: Dict[str, List[str]] = {
    "decimal(22,9)": TYPE_NUMBER,
    "decimal": TYPE_NUMBER,
    "integer": TYPE_NUMBER,
    "int": TYPE_NUMBER,
    "bigint": TYPE_NUMBER,
    "smallint": TYPE_NUMBER,
    "double": TYPE_NUMBER,
    "float": TYPE_NUMBER,
    "number": TYPE_NUMBER,
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


def json_type(zuora_type: Optional[str]) -> List[str]:
    """Map a Zuora column/field type onto a JSON-schema type, defaulting to string."""
    return TYPE_MAPPING.get((zuora_type or "").strip().lower(), TYPE_STRING)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest unit_tests/test_types.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Point `zuora_client.py` at the shared module**

In `source_zuora/zuora_client.py`, delete the `TYPE_NUMBER`/`TYPE_STRING`/`TYPE_OBJECT`/`TYPE_ARRAY`/`TYPE_BOOL` constants and the whole `TYPE_MAPPING` dict (currently lines 19-51), and add to the imports:

```python
from .zuora_types import json_type
```

Then change `describe_object` to use it:

```python
        result = {
            row["Column"]: {"type": json_type(row.get("Type"))}
            for row in self.run_query(f"DESCRIBE {name}")
        }
```

- [ ] **Step 6: Run the whole suite to confirm no regression**

Run: `poetry run pytest unit_tests -v`
Expected: PASS — all pre-existing tests still green.

- [ ] **Step 7: Commit**

```bash
git add source_zuora/zuora_types.py source_zuora/zuora_client.py unit_tests/test_types.py
git commit -m "fix: casefold Zuora type lookup and add missing 'number' type

Zuora's Describe API returns 'ZOQL' uppercase and a 'number' type that was
absent from TYPE_MAPPING; both silently fell through to string. Moves the map
to zuora_types so both query backends can share it."
```

---

### Task 2: Extract the shared HTTP client

Both backends need byte-identical retry, backoff, `Retry-After` handling, and 401/403 classification. Extract it rather than duplicate it.

**Files:**
- Create: `source_zuora/zuora_http.py`
- Modify: `source_zuora/zuora_client.py:125-164` (delete `_sleep_before_retry` and `_request`, delegate to the new client)
- Test: `unit_tests/test_http.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `ZuoraHttpClient(url_base: str, authenticator: Any, session: Optional[requests.Session] = None, request_timeout: tuple = (30, 300), max_retries: int = 5, backoff_factor: float = 1.0)`
  - `.url_base -> str`
  - `.headers() -> Mapping[str, str]` — auth header plus `Content-Type: application/json`
  - `.request(method: str, url: str, **kwargs) -> requests.Response`
  - `.sleep_before_retry(attempt: int, response: Optional[requests.Response]) -> None`
  - Module constants `RETRYABLE_STATUS: set`, `RETRYABLE_EXCEPTIONS: tuple`

- [ ] **Step 1: Write the failing test**

Create `unit_tests/test_http.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest unit_tests/test_http.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'source_zuora.zuora_http'`

- [ ] **Step 3: Write minimal implementation**

Create `source_zuora/zuora_http.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest unit_tests/test_http.py -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Delegate from `ZuoraQueryClient`**

In `source_zuora/zuora_client.py`: delete the module-level `_RETRYABLE_STATUS` and `_RETRYABLE_EXCEPTIONS`, delete the `_sleep_before_retry` and `_request` methods, drop the now-unused `time` and `ZuoraConfigError` imports, and add:

```python
from .zuora_http import ZuoraHttpClient
```

In `__init__`, replace the `self._session`, `self._request_timeout`, `self._max_retries`, `self._backoff_factor` assignments with:

```python
        self._http = ZuoraHttpClient(
            url_base=url_base,
            authenticator=authenticator,
            session=session,
            request_timeout=request_timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )
```

Keep `self._url_base = url_base` and `self._auth = authenticator` so existing call sites keep working. Then replace the two helper methods with thin delegates and update the three call sites:

```python
    def _headers(self) -> Mapping[str, str]:
        return self._http.headers()

    def _sleep_before_retry(self, attempt: int, response=None) -> None:
        self._http.sleep_before_retry(attempt, response)

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        return self._http.request(method, url, **kwargs)
```

- [ ] **Step 6: Run the whole suite**

Run: `poetry run pytest unit_tests -v`
Expected: PASS — `test_client.py` unchanged and still green, proving the extraction was behaviour-preserving.

- [ ] **Step 7: Commit**

```bash
git add source_zuora/zuora_http.py source_zuora/zuora_client.py unit_tests/test_http.py
git commit -m "refactor: extract shared ZuoraHttpClient from ZuoraQueryClient

Both query backends need identical retry, backoff and auth-failure handling."
```

---

### Task 3: Share the transient-job-error classifier

Both backends inspect a terminal job's message to decide whether the failure was a transient Zuora outage worth retrying.

**Files:**
- Modify: `source_zuora/zuora_errors.py` (append)
- Modify: `source_zuora/zuora_client.py:54-68` (delete the local copy, import instead)
- Test: `unit_tests/test_errors.py` (append)

**Interfaces:**
- Consumes: nothing.
- Produces: `is_transient_job_error(message: Optional[str]) -> bool` in `source_zuora.zuora_errors`.

- [ ] **Step 1: Write the failing test**

Append to `unit_tests/test_errors.py`:

```python
from source_zuora.zuora_errors import is_transient_job_error


def test_is_transient_job_error_matches_known_markers():
    assert is_transient_job_error("Internal message: Service Temporarily Unavailable LINK_30000007")
    assert is_transient_job_error("SERVICE UNAVAILABLE")
    assert is_transient_job_error("Please try again later")
    assert is_transient_job_error("Internal Server Error")


def test_is_transient_job_error_rejects_permanent_failures():
    assert not is_transient_job_error("There is a syntax error in one of the queries")
    assert not is_transient_job_error("You must specify a select.")
    assert not is_transient_job_error("")
    assert not is_transient_job_error(None)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest unit_tests/test_errors.py -v`
Expected: FAIL — `ImportError: cannot import name 'is_transient_job_error'`

- [ ] **Step 3: Write minimal implementation**

Append to `source_zuora/zuora_errors.py`:

```python
# Substrings in a terminal job's error message that indicate a transient Zuora-side
# outage (the whole job should be retried) rather than a permanent query/config error.
# e.g. "Internal message: Service Temporarily Unavailable ... LINK_30000007".
TRANSIENT_JOB_MARKERS = (
    "temporarily unavailable",
    "service unavailable",
    "try again",
    "internal server error",
)


def is_transient_job_error(message: Optional[str]) -> bool:
    """True if a terminal job's message names a transient Zuora-side outage."""
    lowered = (message or "").lower()
    return any(marker in lowered for marker in TRANSIENT_JOB_MARKERS)
```

Change the module's typing import to `from typing import Any, Optional`.

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest unit_tests/test_errors.py -v`
Expected: PASS

- [ ] **Step 5: Delete the duplicate in `zuora_client.py`**

Delete the `_TRANSIENT_JOB_MARKERS` tuple and the `_is_transient_job_error` function (currently lines 54-68). Add `is_transient_job_error` to the existing `from .zuora_errors import (...)` block, and change the one call site in `poll_job` from `_is_transient_job_error(message)` to `is_transient_job_error(message)`.

- [ ] **Step 6: Run the whole suite**

Run: `poetry run pytest unit_tests -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add source_zuora/zuora_errors.py source_zuora/zuora_client.py unit_tests/test_errors.py
git commit -m "refactor: share transient-job-error classification between backends"
```

---

### Task 4: Parse the AQuA describe XML

Spec F5, F10, F11. Pure functions, no HTTP — the correctness-critical logic tested directly.

**Files:**
- Create: `source_zuora/zuora_describe.py`
- Test: `unit_tests/test_describe.py`

**Interfaces:**
- Consumes: nothing.
- Produces, all in `source_zuora.zuora_describe`:
  - `parse_object_names(xml: bytes) -> List[str]` — canonical object names from `/v1/describe`
  - `parse_fields(xml: bytes) -> Dict[str, str]` — export-context fields only, canonical field name -> Zuora type string
  - `parse_relationships(xml: bytes) -> List[str]` — relationship names from `<related-objects>`
  - `foreign_key_columns(fields: Mapping[str, str], relationships: Iterable[str]) -> Dict[str, str]` — ordered map of relationship name -> derived lowercase column name, excluding relationships whose derived name already exists as an own field

- [ ] **Step 1: Write the failing test**

Create `unit_tests/test_describe.py`:

```python
from source_zuora.zuora_describe import (
    foreign_key_columns,
    parse_fields,
    parse_object_names,
    parse_relationships,
)

# Shape taken verbatim from GET /v1/describe on an APAC sandbox.
OBJECTS_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<objects>
    <object href="https://x/describe/Account">
     <name>Account</name>
     <label>Account</label>
</object>
    <object href="https://x/describe/Subscription">
     <name>Subscription</name>
     <label>Subscription</label>
</object>
</objects>
"""

# Shape taken verbatim from GET /v1/describe/Subscription. Note `Notes` is
# soap-only (no export context) and must be dropped.
SUBSCRIPTION_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<object href="https://x/describe/Subscription">
   <name>Subscription</name>
   <label>Subscription</label>
   <fields>
      <field>
         <name>Id</name>
         <type>text</type>
         <contexts><context>soap</context><context>export</context></contexts>
      </field>
      <field>
         <name>SubscriptionId</name>
         <type>text</type>
         <contexts><context>export</context></contexts>
      </field>
      <field>
         <name>TermStartDate</name>
         <type>date</type>
         <contexts><context>export</context></contexts>
      </field>
      <field>
         <name>Notes</name>
         <type>text</type>
         <contexts><context>soap</context></contexts>
      </field>
      <field>
         <name>Custom__c</name>
         <type>picklist</type>
         <contexts><context>export</context></contexts>
      </field>
   </fields>
   <related-objects>
      <object href="https://x/describe/Account">
         <name>Account</name>
         <label>Account</label>
      </object>
      <object href="https://x/describe/Contact">
         <name>BillToContact</name>
         <label>Bill To</label>
      </object>
      <object href="https://x/describe/Subscription">
         <name>Subscription</name>
         <label>Subscription</label>
      </object>
   </related-objects>
</object>
"""

NO_FIELDS_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<object><name>BillingPreviewRun</name><label>x</label><fields></fields></object>
"""


def test_parse_object_names():
    assert parse_object_names(OBJECTS_XML) == ["Account", "Subscription"]


def test_parse_fields_keeps_only_export_context():
    fields = parse_fields(SUBSCRIPTION_XML)
    assert fields == {
        "Id": "text",
        "SubscriptionId": "text",
        "TermStartDate": "date",
        "Custom__c": "picklist",
    }
    assert "Notes" not in fields  # soap-only


def test_parse_fields_empty_when_no_export_fields():
    assert parse_fields(NO_FIELDS_XML) == {}


def test_parse_relationships():
    assert parse_relationships(SUBSCRIPTION_XML) == ["Account", "BillToContact", "Subscription"]


def test_parse_relationships_absent_section():
    assert parse_relationships(NO_FIELDS_XML) == []


def test_foreign_key_columns_derives_lowercase_names():
    fields = {"Id": "text", "TermStartDate": "date"}
    assert foreign_key_columns(fields, ["Account", "BillToContact"]) == {
        "Account": "accountid",
        "BillToContact": "billtocontactid",
    }


def test_foreign_key_columns_skips_collision_with_own_field():
    # Subscription owns `SubscriptionId`, so the `Subscription` relationship would
    # emit a duplicate `subscriptionid` column. Spec F11.
    fields = parse_fields(SUBSCRIPTION_XML)
    fks = foreign_key_columns(fields, parse_relationships(SUBSCRIPTION_XML))
    assert fks == {"Account": "accountid", "BillToContact": "billtocontactid"}
    assert "Subscription" not in fks
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest unit_tests/test_describe.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'source_zuora.zuora_describe'`

- [ ] **Step 3: Write minimal implementation**

Create `source_zuora/zuora_describe.py`:

```python
#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""
Parsers for Zuora's Describe API (`GET /v1/describe`, `GET /v1/describe/{object}`),
which returns `text/xml` with no JSON alternative. Pure functions over bytes so the
correctness-critical field and relationship handling is directly testable.
"""

import xml.etree.ElementTree as ET
from typing import Dict, Iterable, List, Mapping

# Only fields whose <contexts> include this are selectable in an Export ZOQL query.
EXPORT_CONTEXT = "export"


def parse_object_names(xml: bytes) -> List[str]:
    """Canonical object names from `GET /v1/describe`."""
    root = ET.fromstring(xml)
    return [name for name in (o.findtext("name") for o in root.findall("object")) if name]


def parse_fields(xml: bytes) -> Dict[str, str]:
    """
    Export-context fields from `GET /v1/describe/{object}`, as canonical field
    name -> Zuora type. Fields lacking the `export` context exist in the object
    model but fail an Export ZOQL query, so they are dropped.
    """
    root = ET.fromstring(xml)
    fields = {}
    for field in root.findall("fields/field"):
        name = field.findtext("name")
        if not name:
            continue
        contexts = {c.text for c in field.findall("contexts/context")}
        if EXPORT_CONTEXT in contexts:
            fields[name] = field.findtext("type") or ""
    return fields


def parse_relationships(xml: bytes) -> List[str]:
    """
    Relationship names from the `<related-objects>` section. Foreign keys are not
    plain columns in Export ZOQL; they are selected as `<Relationship>.Id`. An
    unknown relationship name is a submit-time error, so these are never guessed.
    """
    root = ET.fromstring(xml)
    return [
        name
        for name in (o.findtext("name") for o in root.findall("related-objects/object"))
        if name
    ]


def foreign_key_columns(
    fields: Mapping[str, str], relationships: Iterable[str]
) -> Dict[str, str]:
    """
    Map each usable relationship to the column name its `<Rel>.Id` selection will
    produce, matching Data Query's naming (`Account` -> `accountid`).

    Relationships whose derived name is already an own field are skipped: some
    objects own e.g. `SubscriptionId` *and* have a `Subscription` relationship, and
    selecting both would emit a duplicate column.
    """
    own = {name.lower() for name in fields}
    columns = {}
    for relationship in relationships:
        column = f"{relationship.lower()}id"
        if column in own or column in columns.values():
            continue
        columns[relationship] = column
    return columns
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest unit_tests/test_describe.py -v`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add source_zuora/zuora_describe.py unit_tests/test_describe.py
git commit -m "feat: parse Zuora Describe API XML for objects, export fields and FKs"
```

---

### Task 5: Define the backend interface

**Files:**
- Create: `source_zuora/zuora_backend.py`
- Test: `unit_tests/test_backend.py`

**Interfaces:**
- Consumes: `ZuoraQueryClient` from Task 2's modified `zuora_client.py`.
- Produces:
  - `QueryBackend` ABC with abstract `list_objects()`, `describe_object(name)`, `warm_describe_cache(names)`, `read_object(name, cursor=None, start=None, end=None)`
  - `DATA_QUERY = "Data Query"`, `AQUA = "AQuA"`
  - `get_backend(config: Mapping[str, Any], authenticator: Any, url_base: str) -> QueryBackend`

Note: `get_backend` takes the already-built authenticator and `url_base` rather than building them, so `source.py` keeps ownership of `ZuoraAuthenticator` and the factory stays trivially testable. The AQuA branch is added in Task 7; here it raises so the interface can land and be reviewed on its own.

- [ ] **Step 1: Write the failing test**

Create `unit_tests/test_backend.py`:

```python
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
    backend = get_backend({}, FakeAuth(), BASE)
    assert isinstance(backend, ZuoraQueryClient)


def test_get_backend_selects_data_query_explicitly():
    backend = get_backend({"query_api": DATA_QUERY}, FakeAuth(), BASE)
    assert isinstance(backend, ZuoraQueryClient)


def test_get_backend_passes_data_query_mode_through():
    backend = get_backend({"data_query": "Unlimited"}, FakeAuth(), BASE)
    assert backend._data_query == "Unlimited"


def test_get_backend_rejects_unknown_query_api():
    with pytest.raises(ValueError, match="Unknown query_api"):
        get_backend({"query_api": "Telepathy"}, FakeAuth(), BASE)


def test_aqua_constant_is_the_spec_enum_value():
    assert AQUA == "AQuA"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest unit_tests/test_backend.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'source_zuora.zuora_backend'`

- [ ] **Step 3: Write minimal implementation**

Create `source_zuora/zuora_backend.py`:

```python
#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Iterator, List, Mapping, Optional

DATA_QUERY = "Data Query"
AQUA = "AQuA"


class QueryBackend(ABC):
    """
    One Zuora extraction API. Owns its query dialect, object/field naming, and
    transport, so the streams in `source.py` deal only in lowercase names,
    `datetime` bounds, and normalized records.
    """

    @abstractmethod
    def list_objects(self) -> List[str]:
        """Queryable object names, lowercased, as stream names."""

    @abstractmethod
    def describe_object(self, name: str) -> Mapping[str, Mapping[str, Any]]:
        """Lowercase field name -> `{"type": <json schema type>}` for one object."""

    @abstractmethod
    def warm_describe_cache(self, names: List[str]) -> None:
        """Pre-populate schemas for many objects concurrently."""

    @abstractmethod
    def read_object(
        self,
        name: str,
        cursor: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> Iterator[Mapping[str, Any]]:
        """
        Records for one object. With `cursor`, `start` and `end` set, reads that
        half-open-ordered window; otherwise reads the whole object. Each backend
        renders the bounds in its own dialect — the caller passes `datetime`s.
        """


def get_backend(config: Mapping[str, Any], authenticator: Any, url_base: str) -> QueryBackend:
    """Build the query backend named by `config["query_api"]` (default Data Query)."""
    from .zuora_client import ZuoraQueryClient

    query_api = config.get("query_api") or DATA_QUERY
    if query_api == DATA_QUERY:
        return ZuoraQueryClient(
            url_base=url_base,
            authenticator=authenticator,
            data_query=config.get("data_query", "Live"),
        )
    raise ValueError(f"Unknown query_api {query_api!r}")
```

- [ ] **Step 4: Declare `ZuoraQueryClient` a `QueryBackend`**

In `source_zuora/zuora_client.py`, add the import and change the class declaration:

```python
from .zuora_backend import QueryBackend
```

```python
class ZuoraQueryClient(QueryBackend):
```

`read_object` does not exist yet, so the class is still abstract and cannot be instantiated — Task 6 adds it. Expect `test_backend.py` and `test_client.py` to fail at this step; that is the next task's red state.

- [ ] **Step 5: Commit the interface**

```bash
git add source_zuora/zuora_backend.py unit_tests/test_backend.py source_zuora/zuora_client.py
git commit -m "feat: add QueryBackend interface and get_backend factory

Data Query only for now; the AQuA branch lands with its backend."
```

---

### Task 6: Give the Data Query backend `read_object`

Moves the two ZOQL builders out of `source.py` so the dialect lives with the backend that speaks it.

**Files:**
- Modify: `source_zuora/zuora_client.py`
- Test: `unit_tests/test_client.py` (append)

**Interfaces:**
- Consumes: `QueryBackend` (Task 5).
- Produces on `ZuoraQueryClient`:
  - `render_query(name: str, cursor: Optional[str] = None, start: Optional[datetime] = None, end: Optional[datetime] = None) -> str`
  - `read_object(name, cursor=None, start=None, end=None) -> Iterator[Mapping[str, Any]]`

- [ ] **Step 1: Write the failing test**

Append to `unit_tests/test_client.py`:

```python
import pendulum


def test_render_query_full_refresh():
    assert make_client().render_query("account") == "select * from account"


def test_render_query_incremental_uses_timestamp_literals():
    start = pendulum.datetime(2026, 8, 1, tz="UTC")
    end = pendulum.datetime(2026, 8, 20, tz="UTC")
    query = make_client().render_query("account", cursor="updateddate", start=start, end=end)
    assert query == (
        "select * from account where "
        "updateddate >= TIMESTAMP '2026-08-01 00:00:00.000000 UTC' and "
        "updateddate <= TIMESTAMP '2026-08-20 00:00:00.000000 UTC' "
        "order by updateddate asc"
    )


def test_render_query_ignores_cursor_without_bounds():
    assert make_client().render_query("account", cursor="updateddate") == "select * from account"


def test_read_object_full_refresh(requests_mock):
    register_job(requests_mock, ["completed"])
    requests_mock.get("https://s3/result.jsonl", text='{"id": "a"}\n')
    assert list(make_client().read_object("account")) == [{"id": "a"}]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest unit_tests/test_client.py -v`
Expected: FAIL — `TypeError: Can't instantiate abstract class ZuoraQueryClient with abstract method read_object`

- [ ] **Step 3: Write minimal implementation**

Add to `source_zuora/zuora_client.py` — import `datetime` and `Optional` if not already present, then add these two methods to `ZuoraQueryClient`:

```python
    @staticmethod
    def _to_datetime_str(date: datetime) -> str:
        # e.g. '2021-07-15 07:45:55.000000 -07:00' — format Zuora Data Query accepts
        # as a TIMESTAMP literal.
        return date.strftime("%Y-%m-%d %H:%M:%S.%f %Z")

    def render_query(
        self,
        name: str,
        cursor: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> str:
        if not (cursor and start and end):
            return f"select * from {name}"
        return (
            f"select * from {name} where "
            f"{cursor} >= TIMESTAMP '{self._to_datetime_str(start)}' and "
            f"{cursor} <= TIMESTAMP '{self._to_datetime_str(end)}' "
            f"order by {cursor} asc"
        )

    def read_object(
        self,
        name: str,
        cursor: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> Iterator[Mapping[str, Any]]:
        yield from self.run_query(self.render_query(name, cursor, start, end))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest unit_tests -v`
Expected: PASS — including `test_backend.py`, which can now instantiate the client.

- [ ] **Step 5: Commit**

```bash
git add source_zuora/zuora_client.py unit_tests/test_client.py
git commit -m "feat: move ZOQL query construction into the Data Query backend"
```

---

### Task 7: The AQuA backend

The substance of the feature. Spec F1–F5, F7, F10, F11 and decisions D1, D2, D6.

**Files:**
- Create: `source_zuora/zuora_aqua_client.py`
- Modify: `source_zuora/zuora_backend.py` (wire the `AQUA` branch)
- Test: `unit_tests/test_aqua_client.py`

**Interfaces:**
- Consumes: `ZuoraHttpClient` (Task 2), `is_transient_job_error` (Task 3), `parse_object_names` / `parse_fields` / `parse_relationships` / `foreign_key_columns` (Task 4), `QueryBackend` (Task 5), `json_type` / `TYPE_STRING` (Task 1).
- Produces:
  - `render_query(obj: str, foreign_keys: Iterable[str], cursor: Optional[str] = None, start: Optional[datetime] = None, end: Optional[datetime] = None) -> str` — module-level pure function; `cursor` is the **canonical** field name
  - `normalize_row(obj: str, row: Mapping[str, Optional[str]], types: Mapping[str, List[str]]) -> Dict[str, Any]` — module-level pure function
  - `ZuoraAquaClient(url_base, authenticator, session=None, poll_interval=1.0, max_poll_attempts=1800, request_timeout=(30, 300), max_retries=5, max_job_retries=3, backoff_factor=1.0, describe_concurrency=10)`

- [ ] **Step 1: Write the failing tests for the pure functions**

Create `unit_tests/test_aqua_client.py`:

```python
import pendulum
import pytest

from source_zuora.zuora_aqua_client import ZuoraAquaClient, normalize_row, render_query
from source_zuora.zuora_errors import (
    ZOQLQueryCannotProcessObject,
    ZOQLQueryFailed,
    ZuoraTransientError,
)
from source_zuora.zuora_types import TYPE_BOOL, TYPE_NUMBER, TYPE_STRING

BASE = "https://rest.test.ap.zuora.com"


class FakeAuth:
    def get_auth_header(self):
        return {"Authorization": "Bearer test"}


# --- render_query (spec F3, F10) ---


def test_render_query_full_refresh_selects_star_and_foreign_keys():
    assert render_query("Subscription", ["Account", "BillToContact"]) == (
        "select *, Account.Id, BillToContact.Id from Subscription"
    )


def test_render_query_without_foreign_keys():
    assert render_query("Account", []) == "select * from Account"


def test_render_query_incremental_uses_iso_literals_with_offset():
    # A space-separated literal is accepted by Zuora and silently returns the whole
    # table, so the exact rendered predicate is pinned here. Spec F3.
    start = pendulum.datetime(2026, 8, 1, tz="Australia/Melbourne")
    end = pendulum.datetime(2026, 8, 20, tz="Australia/Melbourne")
    query = render_query("Subscription", ["Account"], cursor="UpdatedDate", start=start, end=end)
    assert query == (
        "select *, Account.Id from Subscription "
        "where UpdatedDate >= '2026-08-01T00:00:00+10:00' "
        "and UpdatedDate <= '2026-08-20T00:00:00+10:00' "
        "order by UpdatedDate asc"
    )


def test_render_query_never_emits_a_space_separated_literal():
    start = pendulum.datetime(2026, 8, 1, tz="UTC")
    query = render_query("Account", [], cursor="UpdatedDate", start=start, end=start)
    assert "'2026-08-01 00:00:00" not in query
    assert "T" in query.split("'")[1]


def test_render_query_ignores_cursor_without_bounds():
    assert render_query("Account", [], cursor="UpdatedDate") == "select * from Account"


# --- normalize_row (spec F7, F10, F11) ---

TYPES = {
    "id": TYPE_STRING,
    "autopay": TYPE_BOOL,
    "balance": TYPE_NUMBER,
    "additionalemailaddresses": TYPE_STRING,
    "accountid": TYPE_STRING,
    "billtocontactid": TYPE_STRING,
}


def test_normalize_row_strips_own_object_prefix_and_lowercases():
    row = {"Subscription.Id": "abc", "Subscription.AutoPay": "false"}
    assert normalize_row("Subscription", row, TYPES) == {"id": "abc", "autopay": False}


def test_normalize_row_joins_relationship_prefix_into_fk_name():
    # `Account.Id` on a Subscription query is the FK, not the row's own id.
    row = {"Subscription.Id": "sub-1", "Account.Id": "acc-1", "BillToContact.Id": "con-1"}
    assert normalize_row("Subscription", row, TYPES) == {
        "id": "sub-1",
        "accountid": "acc-1",
        "billtocontactid": "con-1",
    }


def test_normalize_row_maps_empty_string_to_none():
    row = {"Account.AdditionalEmailAddresses": ""}
    assert normalize_row("Account", row, TYPES) == {"additionalemailaddresses": None}


def test_normalize_row_coerces_numbers_and_booleans():
    row = {"Account.Balance": "-11095", "Account.AutoPay": "true"}
    assert normalize_row("Account", row, TYPES) == {"balance": -11095, "autopay": True}


def test_normalize_row_keeps_fractional_numbers_as_float():
    assert normalize_row("Account", {"Account.Balance": "12.50"}, TYPES)["balance"] == 12.5


def test_normalize_row_leaves_unparseable_number_as_string():
    assert normalize_row("Account", {"Account.Balance": "n/a"}, TYPES)["balance"] == "n/a"


def test_normalize_row_passes_through_unknown_columns_as_strings():
    assert normalize_row("Account", {"Account.Mystery": "x"}, TYPES) == {"mystery": "x"}


def test_normalize_row_ignores_unprefixed_and_none_columns():
    assert normalize_row("Account", {"bare": "x", None: ["trailing"]}, TYPES) == {"bare": "x"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest unit_tests/test_aqua_client.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'source_zuora.zuora_aqua_client'`

- [ ] **Step 3: Write the module**

Create `source_zuora/zuora_aqua_client.py`:

```python
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
        if not isinstance(header, str) or "." not in header:
            if isinstance(header, str):
                record[header.lower()] = value or None
            continue
        prefix, _, field = header.partition(".")
        key = field.lower() if prefix.lower() == obj.lower() else f"{prefix}{field}".lower()
        if value is None or value == "":
            record[key] = None
            continue
        record[key] = _coerce(value, list(types.get(key, TYPE_STRING)))
    return record


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


def _canonical_field(fields: Mapping[str, str], cursor: Optional[str]) -> Optional[str]:
    """Resolve a lowercase cursor name back to its canonical Export ZOQL spelling."""
    if not cursor:
        return None
    for field in fields:
        if field.lower() == cursor.lower():
            return field
    return cursor


def _first_reason(data: Mapping[str, Any]) -> str:
    reasons = data.get("reasons") or []
    return reasons[0].get("message", "") if reasons else ""


def _job_message(data: Mapping[str, Any], batches: List[Mapping[str, Any]]) -> str:
    for batch in batches:
        message = batch.get("message")
        if message:
            return message
    return data.get("message") or ""
```

- [ ] **Step 4: Run the pure-function tests**

Run: `poetry run pytest unit_tests/test_aqua_client.py -v`
Expected: PASS (13 tests)

- [ ] **Step 5: Write the failing job-lifecycle and discovery tests**

Append to `unit_tests/test_aqua_client.py`:

```python
# --- discovery and job lifecycle ---

OBJECTS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<objects>
  <object><name>Account</name><label>Account</label></object>
  <object><name>BillingPreviewRun</name><label>x</label></object>
</objects>
"""

ACCOUNT_XML = """<?xml version="1.0" encoding="UTF-8"?>
<object>
  <name>Account</name><label>Account</label>
  <fields>
    <field><name>Id</name><type>text</type>
      <contexts><context>export</context></contexts></field>
    <field><name>Balance</name><type>decimal</type>
      <contexts><context>export</context></contexts></field>
    <field><name>UpdatedDate</name><type>datetime</type>
      <contexts><context>export</context></contexts></field>
    <field><name>Secret</name><type>text</type>
      <contexts><context>soap</context></contexts></field>
  </fields>
  <related-objects>
    <object><name>ParentAccount</name><label>Parent</label></object>
  </related-objects>
</object>
"""

EMPTY_XML = """<?xml version="1.0" encoding="UTF-8"?>
<object><name>BillingPreviewRun</name><label>x</label><fields></fields></object>
"""


def make_aqua(**kwargs):
    kwargs.setdefault("poll_interval", 0)
    kwargs.setdefault("backoff_factor", 0)
    return ZuoraAquaClient(BASE, FakeAuth(), **kwargs)


def register_describe(requests_mock):
    requests_mock.get(f"{BASE}/v1/describe", text=OBJECTS_XML)
    requests_mock.get(f"{BASE}/v1/describe/Account", text=ACCOUNT_XML)
    requests_mock.get(f"{BASE}/v1/describe/BillingPreviewRun", text=EMPTY_XML)


def register_job(requests_mock, statuses, file_id="file-1", message="", csv_body=""):
    requests_mock.post(f"{BASE}/v1/batch-query/", json={"id": "job-1", "status": "submitted"})
    responses = []
    for status in statuses:
        batch = {"status": status, "recordCount": 1}
        if status == "completed":
            batch["fileId"] = file_id
        if message:
            batch["message"] = message
        responses.append({"json": {"status": status, "batches": [batch]}})
    requests_mock.get(f"{BASE}/v1/batch-query/jobs/job-1", responses)
    requests_mock.get(f"{BASE}/v1/file/{file_id}", text=csv_body)


def test_list_objects_lowercases(requests_mock):
    register_describe(requests_mock)
    assert make_aqua().list_objects() == ["account", "billingpreviewrun"]


def test_describe_object_filters_export_context_and_adds_foreign_keys(requests_mock):
    register_describe(requests_mock)
    schema = make_aqua().describe_object("account")
    assert schema == {
        "id": {"type": TYPE_STRING},
        "balance": {"type": TYPE_NUMBER},
        "updateddate": {"type": TYPE_STRING},
        "parentaccountid": {"type": TYPE_STRING},
    }
    assert "secret" not in schema  # soap-only


def test_describe_object_empty_when_no_export_fields(requests_mock):
    register_describe(requests_mock)
    assert make_aqua().describe_object("billingpreviewrun") == {}


def test_warm_describe_cache_populates_all(requests_mock):
    register_describe(requests_mock)
    client = make_aqua()
    client.warm_describe_cache(["account", "billingpreviewrun"])
    assert set(client._describe_cache) == {"account", "billingpreviewrun"}


def test_submit_sets_use_query_labels_and_zoqlexport(requests_mock):
    register_describe(requests_mock)
    register_job(requests_mock, ["completed"], csv_body="Account.Id\na\n")
    list(make_aqua().read_object("account"))
    body = requests_mock.request_history[-3].json()
    assert body["useQueryLabels"] is True
    assert body["format"] == "csv"
    assert body["version"] == "1.0"
    assert body["queries"][0]["type"] == "zoqlexport"


def test_read_object_normalizes_records(requests_mock):
    register_describe(requests_mock)
    register_job(
        requests_mock,
        ["executing", "completed"],
        csv_body="Account.Id,Account.Balance,ParentAccount.Id\nacc-1,-11095,\n",
    )
    assert list(make_aqua().read_object("account")) == [
        {"id": "acc-1", "balance": -11095, "parentaccountid": None}
    ]


def test_read_object_query_uses_canonical_names_and_foreign_keys(requests_mock):
    register_describe(requests_mock)
    register_job(requests_mock, ["completed"], csv_body="Account.Id\na\n")
    start = pendulum.datetime(2026, 8, 1, tz="UTC")
    list(make_aqua().read_object("account", cursor="updateddate", start=start, end=start))
    query = requests_mock.request_history[-3].json()["queries"][0]["query"]
    assert query == (
        "select *, ParentAccount.Id from Account "
        "where UpdatedDate >= '2026-08-01T00:00:00+00:00' "
        "and UpdatedDate <= '2026-08-01T00:00:00+00:00' "
        "order by UpdatedDate asc"
    )


def test_read_object_handles_quoted_newlines_in_csv(requests_mock):
    register_describe(requests_mock)
    register_job(
        requests_mock,
        ["completed"],
        csv_body='Account.Id,Account.Balance\n"a\nb",1\n',
    )
    assert list(make_aqua().read_object("account")) == [{"id": "a\nb", "balance": 1}]


def test_submit_error_body_with_http_200_raises(requests_mock):
    register_describe(requests_mock)
    requests_mock.post(
        f"{BASE}/v1/batch-query/",
        json={"status": "error", "errorCode": "90005", "message": "There is a syntax error"},
    )
    with pytest.raises(ZOQLQueryFailed, match="syntax error"):
        list(make_aqua().read_object("account"))


def test_submit_malformed_body_with_http_200_raises(requests_mock):
    register_describe(requests_mock)
    requests_mock.post(
        f"{BASE}/v1/batch-query/",
        json={"success": False, "reasons": [{"code": 50000090, "message": "request is malformed"}]},
    )
    with pytest.raises(ZOQLQueryFailed, match="malformed"):
        list(make_aqua().read_object("account"))


def test_aborted_without_select_is_skippable(requests_mock):
    register_describe(requests_mock)
    register_job(requests_mock, ["aborted"], message="java.lang.RuntimeException: You must specify a select.")
    with pytest.raises(ZOQLQueryCannotProcessObject):
        list(make_aqua().read_object("account"))


def test_aborted_transiently_is_retried_then_succeeds(requests_mock):
    register_describe(requests_mock)
    requests_mock.post(f"{BASE}/v1/batch-query/", json={"id": "job-1"})
    requests_mock.get(
        f"{BASE}/v1/batch-query/jobs/job-1",
        [
            {"json": {"status": "aborted",
                      "batches": [{"status": "aborted",
                                   "message": "Service Temporarily Unavailable"}]}},
            {"json": {"status": "completed",
                      "batches": [{"status": "completed", "fileId": "file-1"}]}},
        ],
    )
    requests_mock.get(f"{BASE}/v1/file/file-1", text="Account.Id\nacc-1\n")
    assert list(make_aqua().read_object("account")) == [{"id": "acc-1"}]


def test_aborted_transiently_raises_after_job_retries_exhausted(requests_mock):
    register_describe(requests_mock)
    register_job(requests_mock, ["aborted"], message="Service Temporarily Unavailable")
    with pytest.raises(ZuoraTransientError):
        list(make_aqua(max_job_retries=1).read_object("account"))


def test_aborted_permanently_raises(requests_mock):
    register_describe(requests_mock)
    register_job(requests_mock, ["aborted"], message="The requested data source could not be found")
    with pytest.raises(ZOQLQueryFailed, match="could not be found"):
        list(make_aqua().read_object("account"))
```

- [ ] **Step 6: Run them**

Run: `poetry run pytest unit_tests/test_aqua_client.py -v`
Expected: PASS (27 tests). If `test_submit_sets_use_query_labels_and_zoqlexport` or `test_read_object_query_uses_canonical_names_and_foreign_keys` fails on the `request_history[-3]` index, print `[(r.method, r.url) for r in requests_mock.request_history]` and adjust the index to the `POST /v1/batch-query/` entry — the assertion target is the submit body, not the index.

- [ ] **Step 7: Wire the factory**

In `source_zuora/zuora_backend.py`, replace the `raise ValueError` tail of `get_backend`:

```python
    if query_api == AQUA:
        from .zuora_aqua_client import ZuoraAquaClient

        return ZuoraAquaClient(url_base=url_base, authenticator=authenticator)
    raise ValueError(f"Unknown query_api {query_api!r}")
```

Append to `unit_tests/test_backend.py`:

```python
def test_get_backend_selects_aqua():
    from source_zuora.zuora_aqua_client import ZuoraAquaClient

    assert isinstance(get_backend({"query_api": AQUA}, FakeAuth(), BASE), ZuoraAquaClient)
```

- [ ] **Step 8: Run the whole suite**

Run: `poetry run pytest unit_tests -v`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add source_zuora/zuora_aqua_client.py source_zuora/zuora_backend.py unit_tests/test_aqua_client.py unit_tests/test_backend.py
git commit -m "feat: add AQuA (Export ZOQL) query backend

Selects * plus <Relationship>.Id so foreign keys survive, renders ISO-8601
bounds (a space-separated literal silently returns the whole table), sets
useQueryLabels so headers carry API names, and streams CSV through
DictReader so quoted newlines parse."
```

---

### Task 8: Switch `source.py` onto the backend interface

**Files:**
- Modify: `source_zuora/source.py:116-153` (drop the query builders, call `read_object`), `:85-113` (slices carry `datetime`), `:155-204` (use `get_backend`, filter empty schemas)
- Test: `unit_tests/test_source.py`

**Interfaces:**
- Consumes: `get_backend` (Task 5/7), `read_object` (Tasks 6/7).
- Produces: no new public API. `ZuoraObjectStream.stream_slices` now yields `{"start_date": datetime, "end_date": datetime}`.

- [ ] **Step 1: Write the failing test**

Append to `unit_tests/test_source.py`:

```python
import pendulum
import pytest

from source_zuora.source import SourceZuora, ZuoraObjectStream


class FakeBackend:
    """Records read_object calls so the stream's contract can be asserted."""

    def __init__(self, schema=None, records=None):
        self._schema = schema or {"id": {"type": ["string", "null"]}}
        self._records = records or []
        self.calls = []

    def list_objects(self):
        return ["account"]

    def describe_object(self, name):
        return self._schema

    def warm_describe_cache(self, names):
        pass

    def read_object(self, name, cursor=None, start=None, end=None):
        self.calls.append({"name": name, "cursor": cursor, "start": start, "end": end})
        return iter(self._records)


CONFIG = {"start_date": "2026-08-01", "window_in_days": "10"}


def test_read_records_full_refresh_passes_no_bounds():
    backend = FakeBackend(records=[{"id": "a"}])
    stream = ZuoraObjectStream("account", backend, CONFIG)
    assert list(stream.read_records(sync_mode="full_refresh")) == [{"id": "a"}]
    assert backend.calls == [{"name": "account", "cursor": None, "start": None, "end": None}]


def test_stream_slices_yield_datetimes_not_strings():
    schema = {"id": {"type": ["string", "null"]}, "updateddate": {"type": ["string", "null"]}}
    stream = ZuoraObjectStream("account", FakeBackend(schema=schema), CONFIG)
    first = next(iter(stream.stream_slices()))
    assert isinstance(first["start_date"], datetime)
    assert isinstance(first["end_date"], datetime)


def test_read_records_incremental_forwards_cursor_and_bounds():
    schema = {"id": {"type": ["string", "null"]}, "updateddate": {"type": ["string", "null"]}}
    backend = FakeBackend(schema=schema, records=[{"id": "a", "updateddate": "2026-08-05T00:00:00+00:00"}])
    stream = ZuoraObjectStream("account", backend, CONFIG)
    start = pendulum.datetime(2026, 8, 1, tz="UTC")
    end = pendulum.datetime(2026, 8, 11, tz="UTC")
    records = list(
        stream.read_records(
            sync_mode="incremental", stream_slice={"start_date": start, "end_date": end}
        )
    )
    assert records == [{"id": "a", "updateddate": "2026-08-05T00:00:00+00:00"}]
    assert backend.calls == [
        {"name": "account", "cursor": "updateddate", "start": start, "end": end}
    ]
    assert stream.state == {"updateddate": "2026-08-05T00:00:00+00:00"}


def test_discover_skips_streams_with_empty_schema(monkeypatch):
    # An object with no export-context fields yields no properties and must not be
    # advertised as a stream.
    backend = FakeBackend()
    empty = ZuoraObjectStream("billingpreviewrun", FakeBackend(schema={}), CONFIG)
    good = ZuoraObjectStream("account", backend, CONFIG)
    source = SourceZuora()
    monkeypatch.setattr(source, "streams", lambda config: [good, empty])
    catalog = source.discover(None, {})
    assert [s.name for s in catalog.streams] == ["account"]
```

Add `from datetime import datetime` to the file's imports.

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest unit_tests/test_source.py -v`
Expected: FAIL — `read_records` still calls `self._client.run_query(...)`, and slices still yield strings.

- [ ] **Step 3: Rewrite the stream's query path**

In `source_zuora/source.py`, delete `_to_datetime_str`, `_query_incremental` and `_query_full`. Change `stream_slices` to yield the `datetime` objects directly:

```python
            yield {"start_date": start_date, "end_date": end_slice}
```

Replace `read_records` with:

```python
    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        cursor = self.cursor_field or None
        try:
            if cursor and stream_slice:
                records = self._client.read_object(
                    self.name,
                    cursor=cursor,
                    start=stream_slice.get("start_date"),
                    end=stream_slice.get("end_date"),
                )
            else:
                records = self._client.read_object(self.name)
            for record in records:
                if cursor:
                    incoming = record.get(cursor)
                    if incoming:
                        self._cursor_value = max(self._cursor_value or "", incoming)
                yield record
        except ZOQLQueryCannotProcessObject as error:
            logger.warning("Skipping stream '%s': %s", self.name, error.message)
            return
        except ZOQLQueryFailed as error:
            if "cannot be resolved" not in (error.message or ""):
                raise
            # schema advertised a cursor the query engine rejected — fetch full object
            yield from self._client.read_object(self.name)
```

- [ ] **Step 4: Point the source at the factory and filter empty schemas**

Replace the `ZuoraQueryClient` import with:

```python
from .zuora_backend import get_backend
```

In `streams`:

```python
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = ZuoraAuthenticator(config)
        client = get_backend(config, auth.get_auth(), auth.url_base)
        return [
            ZuoraObjectStream(name, client, config)
            for name in client.list_objects()
            if name not in ZUORA_EXCLUDED_STREAMS
        ]
```

In `discover`, skip objects with no queryable fields (spec D5):

```python
    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        # Pre-fetch every stream's schema concurrently so the per-stream
        # get_json_schema() calls below hit the client cache. One sequential DESCRIBE
        # job per object otherwise blows past Airbyte's discover timeout on large tenants.
        streams = self.streams(config)
        if streams:
            streams[0]._client.warm_describe_cache([stream.name for stream in streams])
        # An object with no queryable fields (AQuA: none carrying the `export`
        # context) cannot be selected from, so it is not advertised as a stream.
        queryable = [s for s in streams if s.get_json_schema().get("properties")]
        return AirbyteCatalog(streams=[stream.as_airbyte_stream() for stream in queryable])
```

In `check_connection`, replace the `ZuoraQueryClient(...)` construction with:

```python
            client = get_backend(config, auth.get_auth(), auth.url_base)
```

- [ ] **Step 5: Run the whole suite**

Run: `poetry run pytest unit_tests -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add source_zuora/source.py unit_tests/test_source.py
git commit -m "refactor: drive streams through the QueryBackend interface

Slices now carry datetimes and each backend renders its own dialect; discover
drops objects with no queryable fields."
```

---

### Task 9: Expose the flag and document it

**Files:**
- Modify: `source_zuora/spec.json`, `README.md`, `metadata.yaml:10`, `pyproject.toml:3`
- Test: `unit_tests/test_spec.py`

**Interfaces:**
- Consumes: `DATA_QUERY` / `AQUA` (Task 5).
- Produces: the `query_api` config property.

- [ ] **Step 1: Write the failing test**

Create `unit_tests/test_spec.py`:

```python
import json
import pathlib

from source_zuora.zuora_backend import AQUA, DATA_QUERY

SPEC = json.loads((pathlib.Path("source_zuora") / "spec.json").read_text())
PROPERTIES = SPEC["connectionSpecification"]["properties"]


def test_query_api_property_exists_with_both_backends():
    assert PROPERTIES["query_api"]["enum"] == [DATA_QUERY, AQUA]


def test_query_api_defaults_to_data_query():
    assert PROPERTIES["query_api"]["default"] == DATA_QUERY


def test_query_api_is_optional_so_saved_configs_stay_valid():
    assert "query_api" not in SPEC["connectionSpecification"]["required"]


def test_data_query_description_scopes_itself_to_the_data_query_backend():
    assert "Data Query" in PROPERTIES["data_query"]["description"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest unit_tests/test_spec.py -v`
Expected: FAIL — `KeyError: 'query_api'`

- [ ] **Step 3: Add the property**

In `source_zuora/spec.json`, insert after the `tenant_endpoint` property:

```json
      "query_api": {
        "title": "Query API",
        "type": "string",
        "description": "Which Zuora API to extract with. `Data Query` (default) uses ZOQL Data Query jobs. `AQuA` uses the Aggregate Query API with Export ZOQL, which exposes a different, smaller set of objects — switching this on an existing connection requires a fresh sync.",
        "enum": ["Data Query", "AQuA"],
        "default": "Data Query"
      },
```

And extend the `data_query` description so its scope is clear:

```json
        "description": "Applies to the `Data Query` API only. Choose between `Live`, or `Unlimited` - the optimized, replicated database at 12 hours freshness for high volume extraction <a href=\"https://knowledgecenter.zuora.com/Central_Platform/Query/Data_Query/A_Overview_of_Data_Query#Query_Processing_Limitations\">Link</a>",
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest unit_tests/test_spec.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Bump the version**

`pyproject.toml:3` -> `version = "0.3.0"`; `metadata.yaml:10` -> `dockerImageTag: 0.3.0`.

- [ ] **Step 6: Document the flag in `README.md`**

Add a section covering: what `query_api` selects; that Data Query stays the default; that AQuA exposes ~119 objects against Data Query's ~188 with only ~85 shared, so switching needs a fresh connection and re-sync; that AQuA reconstructs foreign keys through relationships and roughly a third of them are unavailable; and a pointer to the spec at `docs/superpowers/specs/2026-08-20-zuora-aqua-backend-design.md`.

- [ ] **Step 7: Run the full suite**

Run: `poetry run pytest unit_tests -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add source_zuora/spec.json unit_tests/test_spec.py README.md metadata.yaml pyproject.toml
git commit -m "feat: add query_api flag selecting the Data Query or AQuA backend

Bumps to 0.3.0."
```

---

### Task 10: Verify against a real tenant

Unit tests cannot catch the failure mode that matters most — a silently-dropped predicate (spec F3) looks like a successful sync.

**Files:**
- Create: `integration_tests/aqua_smoke.py`
- Requires: `secrets/config.json` (gitignored) for a sandbox tenant

- [ ] **Step 1: Write the smoke script**

Create `integration_tests/aqua_smoke.py`:

```python
#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""
Manual smoke check against a real tenant. Not part of the unit suite: it needs
`secrets/config.json` and issues real AQuA export jobs.

Run: poetry run python integration_tests/aqua_smoke.py
"""

import json
import pathlib

import pendulum

from source_zuora.zuora_auth import ZuoraAuthenticator
from source_zuora.zuora_backend import AQUA, get_backend

STREAM = "account"


def main() -> None:
    config = json.loads(pathlib.Path("secrets/config.json").read_text())
    auth = ZuoraAuthenticator(config)

    aqua = get_backend({**config, "query_api": AQUA}, auth.get_auth(), auth.url_base)
    objects = aqua.list_objects()
    print(f"AQuA objects: {len(objects)}")

    schema = aqua.describe_object(STREAM)
    foreign_keys = [key for key in schema if key.endswith("id") and key != "id"]
    print(f"{STREAM}: {len(schema)} fields, {len(foreign_keys)} FK columns: {foreign_keys}")

    full = sum(1 for _ in aqua.read_object(STREAM))
    print(f"full refresh rows: {full}")

    start = pendulum.now().subtract(days=7)
    end = pendulum.now()
    windowed = sum(
        1 for _ in aqua.read_object(STREAM, cursor="updateddate", start=start, end=end)
    )
    print(f"last-7-days rows: {windowed}")

    # The whole point of the check: a malformed datetime literal is accepted by
    # Zuora and silently returns every row. Equal counts mean the predicate was
    # dropped, not that the window happened to cover everything.
    assert windowed < full, (
        f"windowed read returned {windowed} of {full} rows — the cursor predicate "
        f"was silently ignored (spec F3)"
    )
    print("OK: incremental predicate is being applied")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it**

Run: `poetry run python integration_tests/aqua_smoke.py`
Expected: object count around 119, a non-empty FK list including `parentaccountid`, and `OK: incremental predicate is being applied`.

If the assertion fires, the rendered predicate is wrong — print the query from `render_query` and compare against the literal formats in spec F3 before changing anything else.

- [ ] **Step 3: Commit**

```bash
git add integration_tests/aqua_smoke.py
git commit -m "test: add AQuA smoke check asserting the cursor predicate applies"
```

---

## Self-Review

**Spec coverage:**

| Spec item | Task |
|---|---|
| F1 CSV only | 7 (`format: "csv"`, `test_submit_sets_use_query_labels_and_zoqlexport`) |
| F2 `useQueryLabels: true` | 7 (same test) |
| F3 ISO datetime literal | 7 (`test_render_query_incremental_uses_iso_literals_with_offset`, `test_render_query_never_emits_a_space_separated_literal`), 10 (row-count assertion) |
| F4 two error layers | 7 (`test_submit_error_body_with_http_200_raises`, `test_submit_malformed_body_with_http_200_raises`, `test_aborted_*`) |
| F5 export-context filtering | 4 (`test_parse_fields_keeps_only_export_context`), 7 (`test_describe_object_filters_export_context_and_adds_foreign_keys`) |
| F6 type map gaps | 1 |
| F7 CSV value shapes | 7 (`normalize_row` tests) |
| F8 case-insensitive names | 7 (`_canonical_name`, `_canonical_field`) |
| F9 divergent object sets | 9 (README + spec description) |
| F10 FK recovery via relationships | 4 (`foreign_key_columns`), 7 (`render_query`, `normalize_row`) |
| F11 collision guard | 4 (`test_foreign_key_columns_skips_collision_with_own_field`) |
| D1 lowercase normalization | 7 (`normalize_row`) |
| D2 stateless + existing windowing | 8 (slices unchanged in shape, `version: "1.0"`) |
| D3 native object sets | 8 (no intersection logic) |
| D4 breaking change documented | 9 |
| D5 skip zero-export-field objects | 8 (`test_discover_skips_streams_with_empty_schema`) |
| D6 `select *` + `<Rel>.Id` | 7 |
| CSV streaming via TextIOWrapper | 7 (`test_read_object_handles_quoted_newlines_in_csv`) |
| Config flag optional, default Data Query | 9 |

**Placeholder scan:** no TBD/TODO; every code step carries the actual code. Task 9 Step 6 (README prose) states the required content rather than final wording — acceptable, it is documentation copy, and the facts it must state are enumerated.

**Type consistency:** `read_object(name, cursor, start, end)` has the same signature in the ABC (Task 5), the Data Query backend (Task 6), the AQuA backend (Task 7), the `FakeBackend` test double (Task 8) and the smoke script (Task 10). `render_query` exists in both backends with different signatures — deliberate, they are separate dialects, and the AQuA one is a module-level function while the Data Query one is a method. `json_type` is used identically in Tasks 1, 6 and 7. `foreign_key_columns` returns `{relationship: column}` and is consumed as `.keys()` for rendering and `.values()` for schema in Task 7, consistent with Task 4.

**Known deviation from the spec:** `TYPE_MAPPING` moves to a new `zuora_types.py` rather than staying in `zuora_client.py`, so the AQuA backend can use it without importing the Data Query backend. Recorded in the File Structure table.
