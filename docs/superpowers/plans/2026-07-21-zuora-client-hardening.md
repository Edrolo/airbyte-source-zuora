# Zuora Client Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox syntax.

**Goal:** Apply the three high-value robustness lessons from the Shopify connector review to `source-zuora`: (1) retry/backoff/timeouts in `ZuoraQueryClient`, (2) an `AirbyteTracedException`-based error taxonomy with correct `FailureType`, (3) a `check_connection` that exercises a real ZOQL call.

**Architecture:** Error taxonomy centralizes in `zuora_errors.py`; the client routes every HTTP call through one retry helper; `check_connection` runs `list_objects()`.

**Tech Stack:** airbyte-cdk 7.23.x, Python 3.10–3.13, Poetry, pytest + requests-mock. No new runtime dependencies (hand-rolled retry).

## Global Constraints

- No new runtime dependencies. Retry is hand-rolled with `time.sleep` + exponential backoff.
- `FailureType` values: `config_error` (user-actionable), `transient_error` (retryable/outage), `system_error` (server-side query failure). Import: `from airbyte_cdk.models import FailureType`, `from airbyte_cdk.utils import AirbyteTracedException`.
- Existing behavior must not regress: the full unit suite (currently 22 tests) stays green; `ZOQLQueryFailed(message, query)` must keep `.message`/`.query` attributes (source.py's read_records inspects `.message` for `"cannot be resolved"`); `ZOQLQueryCannotProcessObject(message)` keeps `.message`.
- Run tests via Poetry; in non-interactive bash `export PATH="$HOME/.local/bin:$PATH"` first.
- TDD: RED then GREEN for each task.

---

### Task 1: Error taxonomy → AirbyteTracedException

**Files:**
- Modify: `source_zuora/zuora_errors.py`
- Modify: `source_zuora/source.py` (add a `logger.warning` where a stream is skipped)
- Test: `unit_tests/test_errors.py` (extend)

**Interfaces:**
- Produces:
  - `ZOQLQueryFailed(message, query="")` — `AirbyteTracedException`, `failure_type=system_error`, attrs `.message` (== raw message), `.query`.
  - `ZOQLQueryCannotProcessObject(message="")` — `AirbyteTracedException`, `failure_type=config_error`, attr `.message`.
  - `ZuoraTransientError(message)` — `AirbyteTracedException`, `failure_type=transient_error`, attr `.message`.
  - `ZuoraConfigError(message)` — `AirbyteTracedException`, `failure_type=config_error`, attr `.message`.
  - `QueryWindowError(value)` — unchanged (already config_error).

- [ ] **Step 1: Extend `unit_tests/test_errors.py` (append these tests)**

```python
def test_zoql_query_failed_is_traced_system_error():
    from airbyte_cdk.models import FailureType
    from airbyte_cdk.utils import AirbyteTracedException
    err = ZOQLQueryFailed("boom", "select * from account")
    assert isinstance(err, AirbyteTracedException)
    assert err.failure_type == FailureType.system_error
    assert err.message == "boom"          # unchanged contract used by read_records
    assert err.query == "select * from account"


def test_cannot_process_object_is_config_error():
    from airbyte_cdk.models import FailureType
    err = ZOQLQueryCannotProcessObject("nope")
    assert err.failure_type == FailureType.config_error
    assert err.message == "nope"


def test_transient_and_config_errors():
    from airbyte_cdk.models import FailureType
    from source_zuora.zuora_errors import ZuoraTransientError, ZuoraConfigError
    assert ZuoraTransientError("net").failure_type == FailureType.transient_error
    assert ZuoraConfigError("bad creds").failure_type == FailureType.config_error
    assert ZuoraTransientError("net").message == "net"
```

- [ ] **Step 2: Run — expect RED**

Run: `poetry run pytest unit_tests/test_errors.py -v`
Expected: FAIL (ZuoraTransientError/ZuoraConfigError don't exist; failure_type attrs missing).

- [ ] **Step 3: Replace `source_zuora/zuora_errors.py`**

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
            message=message, internal_message=message, failure_type=FailureType.config_error
        )


class ZOQLQueryFailed(AirbyteTracedException):
    """A ZOQL Data Query job terminated in a non-completed (server-side) state."""

    def __init__(self, message: str, query: str = ""):
        self.query = query
        super().__init__(
            message=message,
            internal_message=f"{message} | QUERY: {query}",
            failure_type=FailureType.system_error,
        )


class ZOQLQueryCannotProcessObject(AirbyteTracedException):
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
        super().__init__(
            message=message, internal_message=message, failure_type=FailureType.config_error
        )


class ZuoraTransientError(AirbyteTracedException):
    """A Zuora HTTP request failed with a retryable error after retries were exhausted."""

    def __init__(self, message: str):
        super().__init__(
            message=message, internal_message=message, failure_type=FailureType.transient_error
        )


class ZuoraConfigError(AirbyteTracedException):
    """A Zuora request failed for an actionable, user-side reason (bad credentials / access)."""

    def __init__(self, message: str):
        super().__init__(
            message=message, internal_message=message, failure_type=FailureType.config_error
        )
```

> NOTE: `AirbyteTracedException.__init__` sets `self.message` from the `message=` kwarg, so `ZOQLQueryFailed(...).message` is the raw Zuora message — the contract `read_records` relies on. Do not set `self.message` manually (it would be overwritten by super and is unnecessary).

- [ ] **Step 4: Add skip-warning in `source.py`**

In `ZuoraObjectStream.read_records`, change the swallow branch from:

```python
        except ZOQLQueryCannotProcessObject:
            # non-critical: skip this stream
            return
```

to:

```python
        except ZOQLQueryCannotProcessObject as error:
            logger.warning("Skipping stream '%s': %s", self.name, error.message)
            return
```

(`logger = logging.getLogger("airbyte")` already exists at module top in source.py.)

- [ ] **Step 5: Run — expect GREEN**

Run: `poetry run pytest unit_tests/ -v`
Expected: PASS (25 tests: 22 existing + 3 new). Output pristine.

- [ ] **Step 6: Commit**

```bash
git add source_zuora/zuora_errors.py source_zuora/source.py unit_tests/test_errors.py
git commit -m "feat: AirbyteTracedException error taxonomy with FailureType; warn on skipped stream"
```

---

### Task 2: Retry/backoff/timeouts in `ZuoraQueryClient`

**Files:**
- Modify: `source_zuora/zuora_client.py`
- Test: `unit_tests/test_client.py` (extend)

**Interfaces:**
- Consumes: `ZuoraTransientError`, `ZuoraConfigError` from `zuora_errors`.
- Produces: `ZuoraQueryClient(..., request_timeout=(30, 300), max_retries=5, backoff_factor=1.0)`; all HTTP goes through `_request(method, url, **kwargs)` which retries transient failures and raises `ZuoraTransientError` (exhausted) / `ZuoraConfigError` (401/403).

- [ ] **Step 1: Extend `unit_tests/test_client.py` (append)**

```python
def test_request_retries_transient_5xx_then_succeeds(requests_mock):
    from source_zuora.zuora_client import ZuoraQueryClient
    # submit returns 503 once, then 200
    requests_mock.post(
        f"{BASE}/query/jobs",
        [{"status_code": 503}, {"json": {"data": {"id": "job-1"}}, "status_code": 200}],
    )
    requests_mock.get(f"{BASE}/query/jobs/job-1", json={"data": {"queryStatus": "completed", "dataFile": "https://s3/r.jsonl"}})
    requests_mock.get("https://s3/r.jsonl", text='{"id": "a"}\n')
    client = ZuoraQueryClient(BASE, FakeAuth(), poll_interval=0, backoff_factor=0)
    assert list(client.run_query("select 1")) == [{"id": "a"}]


def test_request_raises_transient_after_exhaustion(requests_mock):
    from source_zuora.zuora_client import ZuoraQueryClient
    from source_zuora.zuora_errors import ZuoraTransientError
    requests_mock.post(f"{BASE}/query/jobs", status_code=503)
    client = ZuoraQueryClient(BASE, FakeAuth(), poll_interval=0, backoff_factor=0, max_retries=2)
    with pytest.raises(ZuoraTransientError):
        list(client.run_query("select 1"))


def test_request_maps_401_to_config_error(requests_mock):
    from source_zuora.zuora_client import ZuoraQueryClient
    from source_zuora.zuora_errors import ZuoraConfigError
    requests_mock.post(f"{BASE}/query/jobs", status_code=401)
    client = ZuoraQueryClient(BASE, FakeAuth(), poll_interval=0, backoff_factor=0)
    with pytest.raises(ZuoraConfigError):
        list(client.run_query("select 1"))
```

- [ ] **Step 2: Run — expect RED**

Run: `poetry run pytest unit_tests/test_client.py -v`
Expected: FAIL (no retry today; 503 raises `HTTPError` via `raise_for_status`, 401 not mapped, `backoff_factor`/`max_retries` kwargs unknown).

- [ ] **Step 3: Edit `zuora_client.py`**

Add near the top (after existing imports — `time` is already imported):

```python
import requests

from .zuora_errors import (
    ZOQLQueryCannotProcessObject,
    ZOQLQueryFailed,
    ZuoraConfigError,
    ZuoraTransientError,
)

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_RETRYABLE_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)
```

(Merge the `zuora_errors` import with any existing one — do not duplicate.)

Change `__init__` to accept the new params and store them:

```python
    def __init__(
        self,
        url_base: str,
        authenticator: Any,
        data_query: str = "Live",
        poll_interval: float = 1.0,
        max_poll_attempts: int = 1800,
        session: Optional[requests.Session] = None,
        request_timeout: tuple = (30, 300),
        max_retries: int = 5,
        backoff_factor: float = 1.0,
    ):
        self._url_base = url_base
        self._auth = authenticator
        self._data_query = data_query
        self._poll_interval = poll_interval
        self._max_poll_attempts = max_poll_attempts
        self._session = session or requests.Session()
        self._request_timeout = request_timeout
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor
        self._describe_cache: dict = {}
```

(Preserve any existing fields such as `_describe_cache`/`_max_poll_attempts` from earlier work — keep them; only add the three new fields.)

Add the retry helper:

```python
    def _sleep_before_retry(self, attempt: int, response) -> None:
        delay = self._backoff_factor * (2 ** attempt)
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = float(retry_after)
                except ValueError:
                    pass
        if delay > 0:
            time.sleep(delay)

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", self._request_timeout)
        for attempt in range(self._max_retries + 1):
            try:
                response = self._session.request(method, url, **kwargs)
            except _RETRYABLE_EXCEPTIONS as exc:
                if attempt >= self._max_retries:
                    raise ZuoraTransientError(
                        f"Request {method} {url} failed after {self._max_retries} retries: {exc}"
                    )
                self._sleep_before_retry(attempt, None)
                continue
            if response.status_code in _RETRYABLE_STATUS:
                if attempt >= self._max_retries:
                    raise ZuoraTransientError(
                        f"Request {method} {url} failed with HTTP {response.status_code} "
                        f"after {self._max_retries} retries"
                    )
                self._sleep_before_retry(attempt, response)
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

Route the three existing HTTP calls through `_request` (replace the direct `self._session.post/get` calls):

```python
    def submit_job(self, zoql: str) -> str:
        params = self._base_params()
        params["query"] = zoql
        response = self._request(
            "POST", f"{self._url_base}/query/jobs", headers=self._headers(), json=params
        )
        return response.json()["data"]["id"]
```

In `poll_job`, replace `resp = self._session.get(...)` / `resp.raise_for_status()` with:

```python
            response = self._request(
                "GET", f"{self._url_base}/query/jobs/{job_id}", headers=self._headers()
            )
            data = response.json()["data"]
```

In `_download`, replace the GET with:

```python
        response = self._request("GET", data_file_url, stream=True)
        for line in response.iter_lines():
            if line:
                yield json.loads(line)
```

(Keep the `max_poll_attempts` bound, the `queryStatus` error branching, `_base_params`, `_headers`, `describe_object` memoization, `list_objects` exactly as they are.)

- [ ] **Step 4: Run — expect GREEN**

Run: `poetry run pytest unit_tests/ -v`
Expected: PASS (28 tests: 25 + 3 new). Existing client tests unaffected (they use 2xx responses / job-status failures, not retryable HTTP).

- [ ] **Step 5: Commit**

```bash
git add source_zuora/zuora_client.py unit_tests/test_client.py
git commit -m "feat: retry/backoff + per-call timeouts in ZuoraQueryClient"
```

---

### Task 3: Strengthen `check_connection`

**Files:**
- Modify: `source_zuora/source.py`
- Test: `unit_tests/test_source.py` (extend)

**Interfaces:**
- Consumes: `ZuoraQueryClient`, `ZuoraAuthenticator`, `AirbyteTracedException`.
- Produces: `SourceZuora.check_connection` that returns `(False, msg)` on unknown tenant, empty object list, or a raised `AirbyteTracedException`; `(True, None)` when `list_objects()` returns ≥1 object.

- [ ] **Step 1: Extend `unit_tests/test_source.py` (append)**

```python
def test_check_connection_success(monkeypatch):
    import source_zuora.source as src

    class OkClient:
        def __init__(self, *a, **k):
            pass
        def list_objects(self):
            return ["account", "invoice"]

    monkeypatch.setattr(src, "ZuoraQueryClient", OkClient)
    ok, msg = SourceZuora().check_connection(logger=None, config=CONFIG)
    assert ok is True and msg is None


def test_check_connection_empty_objects_fails(monkeypatch):
    import source_zuora.source as src

    class EmptyClient:
        def __init__(self, *a, **k):
            pass
        def list_objects(self):
            return []

    monkeypatch.setattr(src, "ZuoraQueryClient", EmptyClient)
    ok, msg = SourceZuora().check_connection(logger=None, config=CONFIG)
    assert ok is False and "no queryable objects" in msg.lower()


def test_check_connection_traced_exception_returns_message(monkeypatch):
    import source_zuora.source as src
    from source_zuora.zuora_errors import ZuoraConfigError

    class BadClient:
        def __init__(self, *a, **k):
            pass
        def list_objects(self):
            raise ZuoraConfigError("bad credentials")

    monkeypatch.setattr(src, "ZuoraQueryClient", BadClient)
    ok, msg = SourceZuora().check_connection(logger=None, config=CONFIG)
    assert ok is False and msg == "bad credentials"
```

- [ ] **Step 2: Run — expect RED**

Run: `poetry run pytest unit_tests/test_source.py -v`
Expected: FAIL (current check_connection only builds the OAuth header; success test passes trivially but the empty-objects and traced-exception tests fail because the current code never calls `list_objects`).

- [ ] **Step 3: Edit `SourceZuora.check_connection` in `source.py`**

Ensure `from airbyte_cdk.utils import AirbyteTracedException` is imported at the top, then replace the method with:

```python
    def check_connection(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> Tuple[bool, Optional[Any]]:
        auth = ZuoraAuthenticator(config)
        if not auth.url_base:
            return False, (
                f"Unknown tenant_endpoint {config.get('tenant_endpoint')!r}. "
                f"Choose a valid endpoint from the connector spec."
            )
        try:
            client = ZuoraQueryClient(
                url_base=auth.url_base,
                authenticator=auth.get_auth(),
                data_query=config.get("data_query", "Live"),
            )
            objects = client.list_objects()
        except AirbyteTracedException as error:
            return False, error.message
        except Exception as error:
            return False, f"Unable to connect to Zuora: {error}"
        if not objects:
            return False, (
                "Connected to Zuora but no queryable objects were returned. Confirm the "
                "Data Query feature is enabled and the API user has query permissions."
            )
        return True, None
```

- [ ] **Step 4: Run — expect GREEN**

Run: `poetry run pytest unit_tests/ -v`
Expected: PASS (31 tests: 28 + 3 new). `test_check_connection_invalid_tenant_returns_false` still passes (unknown tenant short-circuits before any network).

- [ ] **Step 5: Verify the connector still runs**

Run: `poetry run source-zuora spec`
Expected: emits SPEC JSON, no traceback.

- [ ] **Step 6: Commit**

```bash
git add source_zuora/source.py unit_tests/test_source.py
git commit -m "feat: check_connection exercises a real ZOQL call (list_objects)"
```

---

## Self-review notes

- Spec coverage: retry/backoff/timeouts (T2), error taxonomy + skip-warning (T1), real-access check (T3). All three approved items mapped.
- Type consistency: `ZuoraTransientError`/`ZuoraConfigError`/`ZOQLQueryFailed(message, query)`/`ZOQLQueryCannotProcessObject(message)` names consistent across tasks; `_request` used by submit/poll/download.
- Ordering: T1 defines the exception types T2 raises, so T1 precedes T2. T3 depends on T2's client but only its `list_objects` (unchanged interface), so ordering T1→T2→T3 is safe.
- Not touched (deferred earlier, still deferred): window-boundary overlap, `requests.Session` lifecycle/close, `TYPE_MAPPING` case-sensitivity, `IncrementalMixin`→`CheckpointMixin`.
