# Design: Modernize `source-zuora` to airbyte-cdk 7.x

**Date:** 2026-07-21
**Status:** Approved (design), pending spec review

## Motivation

The connector is a legacy Airbyte Python CDK source. It fails against the current CDK
(`airbyte-cdk` 7.23.x) and, critically, **does not emit per-stream state**, which
undermines data-integrity guarantees in Airbyte sync operations. The root causes:

- It overrides **private** CDK internals that no longer exist / changed in 7.x:
  `HttpStream._send_request`, and abuses `read_records(sync_mode=None)` as an
  internal RPC mechanism.
- It uses the **removed** `airbyte_cdk.logger.AirbyteLogger`.
- It uses the **deprecated** `get_updated_state` + a single global-state model
  (state keyed only by cursor field, shared across all dynamically-generated
  streams) — so no per-stream `AirbyteStateMessage` is produced.
- Packaging is `setup.py`/`setup.cfg` based; Airbyte's current standard is Poetry
  + `metadata.yaml` (base-image build, no Dockerfile).

## Decisions (confirmed with user)

- **Scope:** Full working migration against `airbyte-cdk` 7.x.
- **Toolchain:** Poetry (Airbyte standard). Include `metadata.yaml` +
  `acceptance-test-config.yml`.
- **State model:** `IncrementalMixin` per stream (per-stream state).
- **Deletions approved:** `setup.py`, `setup.cfg`, `airbyte_source_zuora.egg-info/`,
  `main.py`, `PKG-INFO`.

## Environment constraints

- `airbyte-cdk` 7.23.x requires **Python `>=3.10,<3.14`**.
- Local default Python is **3.14.6** (unsupported by the CDK). `uv` already has
  cpython **3.13.3 / 3.12.12 / 3.11.12** installed locally.
- **Approach:** install Poetry via `uv tool install poetry`; pin the Poetry
  virtualenv to the uv-managed 3.13 interpreter (`poetry env use <path>`).

## Architecture

### Module map (target)

```
source_zuora/
  __init__.py          # exports SourceZuora
  run.py               # entrypoint: launch(SourceZuora(), argv)  (unchanged intent)
  source.py            # SourceZuora(AbstractSource) + ZuoraObjectStream
  zuora_client.py      # NEW: ZuoraQueryClient (submit/poll/download + list/describe)
  zuora_auth.py        # OAuth authenticator (import path refreshed)
  zuora_endpoint.py    # tenant -> url_base map (unchanged)
  zuora_errors.py      # exceptions, refreshed (no AirbyteLogger, no sys.exit)
  zuora_excluded_streams.py  # unchanged
  spec.json            # unchanged (valid draft-07 spec)
```

### 1. `ZuoraQueryClient` (structural fix)

Replaces the fake helper "streams" (`ZuoraSubmitJob`, `ZuoraJobStatusCheck`,
`ZuoraGetJobResult`). A plain object wrapping a `requests.Session` and the OAuth
authenticator.

Interface:

- `run_query(zoql: str) -> Iterator[Mapping[str, Any]]`
  Submit ZOQL job → poll status until terminal → stream JSONL records from the
  `dataFile` URL. Raises the typed ZOQL exceptions (see §4) on server errors.
- `list_objects() -> list[str]` — `SHOW TABLES`.
- `describe_object(name: str) -> Mapping[str, Any]` — `DESCRIBE {name}`, returns a
  JSON-schema `properties` dict (Zuora type -> JSON-schema type mapping preserved
  from `ZuoraDescribeObject.parse_response`).

Config it needs: `url_base`, `authenticator`, `data_query` (LIVE/Unlimited ->
`sourceData: DATAHUB`), request/poll timeouts.

Polling: bounded by Zuora's server-side query runtime limit; loop with a short
sleep between status checks (no infinite loop — server aborts long queries and
returns a terminal error status, mirroring existing behavior).

### 2. `ZuoraObjectStream(Stream, IncrementalMixin)`

Plain `Stream` (not `HttpStream`) — the client owns HTTP.

- `primary_key = "id"`; `cursor_field` resolved per stream (`updateddate`, else
  `createddate`, else full-refresh only).
- **State (per-stream):**
  - `state` getter returns `{cursor_field: self._cursor_value}` (or `{}`).
  - `state` setter stores `self._cursor_value` from the incoming mapping.
- `stream_slices(...)`: date windows from `start_date`/state to now, stepped by
  `window_in_days` (logic preserved, cursor-agnostic — uses whichever cursor the
  stream resolved).
- `read_records(stream_slice, ...)`:
  - Build ZOQL for the resolved cursor + slice; call `client.run_query`.
  - Yield records; advance `self._cursor_value = max(...)` as records arrive.
  - **Cursor fallback** (moved here from `_send_request`): on
    `ZOQLQueryFieldCannotResolveCursor` retry with `createddate`; on
    `...AltCursor` fall back to full-object fetch; on
    `ZOQLQueryCannotProcessObject` skip the stream (yield nothing).
- `get_json_schema()`: `client.describe_object(self.name)`.
- `as_airbyte_stream()`: override to set `default_cursor_field` from schema, and
  mark full-refresh-only streams (no cursor available), as today.
- `state_checkpoint_interval`: retained.

### 3. `SourceZuora(AbstractSource)`

- `check_connection(self, logger: logging.Logger, config) -> tuple[bool, Any]`:
  build authenticator, request token, return `(True, None)` or `(False, error)`.
- `streams(config) -> list[Stream]`: build authenticator + client, `list_objects()`,
  filter `ZUORA_EXCLUDED_STREAMS`, dynamically create a `ZuoraObjectStream`
  subclass per object name (preserving current dynamic-typing approach), pass the
  client in.

### 4. Cross-cutting

- **Logging:** replace `AirbyteLogger` with `logging.getLogger("airbyte")`.
- **Errors:** keep the ZOQL exception taxonomy but remove `AirbyteLogger` and
  `sys.exit(1)` side effects. `QueryWindowError` (bad config) raises
  `AirbyteTracedException(failure_type=FailureType.config_error)`. Control-flow
  exceptions (cursor fallback) stay as plain exceptions caught in `read_records`.
- **`window_in_days`:** parsed from config string as today; invalid -> config error.

### 5. Packaging & Airbyte metadata

- `pyproject.toml`: `[tool.poetry]`, `python = ">=3.10,<3.14"`,
  deps `airbyte-cdk = "^7.23"`, `pendulum`; dev group `pytest`, `requests-mock`,
  `pytest-mock`. Script `source-zuora = "source_zuora.run:run"`. `poetry.lock`
  generated.
- `metadata.yaml`: connector metadata + `connectorBuildOptions.baseImage`
  (`docker.io/airbyte/python-connector-base`), no Dockerfile.
- `acceptance-test-config.yml`: standard CAT config referencing `spec.json`,
  `invalid_config.json`, `configured_catalog.json`, `abnormal_state.json`.
- Delete: `setup.py`, `setup.cfg`, `airbyte_source_zuora.egg-info/`, `main.py`,
  `PKG-INFO`.

## Testing

- `unit_tests/` with `requests-mock`:
  - `ZuoraQueryClient`: submit→poll→download happy path; terminal error statuses
    map to the correct exceptions; JSONL parsing.
  - `ZuoraObjectStream`: `state` get/set round-trip; `stream_slices` windowing;
    cursor-fallback branches; `as_airbyte_stream` cursor/full-refresh selection;
    `describe_object` type mapping.
- Verifiable locally: `poetry run source-zuora spec`, unit tests, and
  discover-structure via mocks.
- **Not** verifiable here: end-to-end `check`/`read` — requires live Zuora
  credentials the developer does not have. This gap will be stated explicitly;
  CAT + live creds run in CI.

## Out of scope (YAGNI)

- Low-code / YAML manifest conversion — infeasible given dynamic stream discovery
  and the ZOQL submit/poll/download job model.
- Any refactor unrelated to the CDK migration.
