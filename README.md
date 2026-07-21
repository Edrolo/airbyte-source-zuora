# Zuora Source Connector (Edrolo)

An [Airbyte](https://airbyte.com/) source connector for [Zuora](https://www.zuora.com/),
built and maintained by **Edrolo**.

> **Attribution & status.** This is an independent, Edrolo-maintained connector, adapted
> from Airbyte's original `source-zuora` (which Airbyte has **archived** at version 0.1.3).
> It is **not** hosted, published, or maintained by Airbyte, and it is not part of the
> Airbyte connector registry. It has been modernized to run on **airbyte-cdk 7.x** with
> per-stream incremental state. Use it at your own discretion.

## What it does

The connector extracts data from Zuora using the [ZOQL Data Query](https://knowledgecenter.zuora.com/Central_Platform/Query/Data_Query)
API. It:

- **Discovers streams dynamically** — every queryable Zuora object (`SHOW TABLES`) becomes a
  stream; its schema is derived on the fly from `DESCRIBE <object>`. There is no hand-maintained
  `schemas/` directory.
- **Syncs incrementally per stream** — each object carries its own cursor and state. The cursor
  resolves from the object's schema: `updateddate` if present, else `createddate`, else the
  stream is full-refresh only.
- **Runs each query as an async job** — submit → poll → download the JSONL result — via a small
  `requests`-based client (`ZuoraQueryClient`) with retries, backoff, and per-call timeouts.

## Authentication

Zuora OAuth2 **client credentials**. You need a Zuora `client_id` / `client_secret` for an API
user with Data Query permissions, and you must know which Zuora tenant endpoint your account
lives on.

## Configuration

Config conforms to [`source_zuora/spec.json`](source_zuora/spec.json):

| Field | Required | Description |
|---|---|---|
| `start_date` | yes | Replication start date, `YYYY-MM-DD`. |
| `tenant_endpoint` | yes | Your Zuora tenant location (e.g. `US Production`, `EU Production`, `US API Sandbox`, …). See the spec for the full enum. |
| `data_query` | yes | `Live` (default) or `Unlimited` (the replicated Data Query store, ~12h freshness, for high-volume extraction). |
| `client_id` | yes | OAuth client ID (secret). |
| `client_secret` | yes | OAuth client secret (secret). |
| `window_in_days` | no | Size of each incremental date slice (default `90`). Larger = fewer, bigger jobs. |

## Local development

Requires [Poetry](https://python-poetry.org/) and **Python 3.10–3.13** (airbyte-cdk does not
support 3.14).

```bash
poetry env use python3.13
poetry install
```

Create a `secrets/config.json` matching `source_zuora/spec.json` (the `secrets/` directory is
git-ignored). Then run the standard Airbyte connector commands:

```bash
poetry run source-zuora spec
poetry run source-zuora check    --config secrets/config.json
poetry run source-zuora discover --config secrets/config.json
poetry run source-zuora read     --config secrets/config.json --catalog integration_tests/configured_catalog.json
```

## Testing

Unit tests use `pytest` + `requests-mock` (no live Zuora account needed):

```bash
poetry run pytest unit_tests/ -v
```

`check`, `discover`, and `read` against a real Zuora tenant require valid credentials in
`secrets/config.json`. `acceptance-test-config.yml` is provided for running Airbyte's
[Connector Acceptance Tests](https://docs.airbyte.com/connector-development/testing-connectors/connector-acceptance-tests-reference)
if you wire them up in your own CI.

## Packaging & Docker

Dependencies are managed with Poetry (`pyproject.toml` / `poetry.lock`) — there is no `setup.py`
and no `requirements.txt`. `metadata.yaml` declares an Airbyte base-image build
(`connectorBuildOptions.baseImage`) rather than a hand-written `Dockerfile`; building a runnable
image is only needed if you deploy this into an Airbyte platform, and requires Airbyte's
connector build tooling.

> If you publish an image, pin the `baseImage` digest in `metadata.yaml` first (it currently
> carries a scaffold value with an inline note).

## Architecture

| File | Responsibility |
|---|---|
| `source_zuora/source.py` | `SourceZuora(AbstractSource)` + `ZuoraObjectStream(Stream, IncrementalMixin)` — discovery, per-stream state, slicing, cursor resolution/fallback. |
| `source_zuora/zuora_client.py` | `ZuoraQueryClient` — ZOQL submit/poll/download, `list_objects`, `describe_object`, retries/backoff/timeouts. |
| `source_zuora/zuora_auth.py` | OAuth2 `client_credentials` authenticator. |
| `source_zuora/zuora_endpoint.py` | Tenant-endpoint → API base-URL mapping. |
| `source_zuora/zuora_errors.py` | `AirbyteTracedException`-based error taxonomy (config / transient / system). |
| `source_zuora/zuora_excluded_streams.py` | Objects to skip during discovery. |

## Known limitations & follow-ups

- **Python 3.14 is not supported** until airbyte-cdk supports it.
- `check` / `discover` / `read` cannot be verified without a live Zuora tenant.
- `ZuoraObjectStream` uses the CDK's `IncrementalMixin`, which is deprecated in favor of
  `CheckpointMixin` — a candidate follow-up before a future CDK bump.
- Minor deferred items: date-window boundaries overlap by one edge (deduplicated by primary key);
  the `requests.Session` is not explicitly closed; `TYPE_MAPPING` assumes lowercase,
  unparameterized Zuora column types.

## Maintainers

Maintained by Edrolo. For issues with this connector, use this repository's issue tracker — not
Airbyte's.
