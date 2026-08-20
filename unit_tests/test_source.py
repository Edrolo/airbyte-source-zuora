from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import CheckpointMixin, IncrementalMixin
from source_zuora.source import ZuoraObjectStream, SourceZuora


def test_stream_uses_checkpointmixin_not_deprecated_incrementalmixin():
    # State/checkpointing must come from CheckpointMixin; IncrementalMixin is deprecated.
    assert issubclass(ZuoraObjectStream, CheckpointMixin)
    assert not issubclass(ZuoraObjectStream, IncrementalMixin)


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

    def read_object(self, name, cursor=None, start=None, end=None):
        self.queries.append({"name": name, "cursor": cursor, "start": start, "end": end})
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


def test_stream_slices_single_slice_when_no_cursor():
    client = FakeClient({"name": {"type": ["string", "null"]}})  # no updateddate/createddate
    stream = make_stream(client)
    assert list(stream.stream_slices(sync_mode=SyncMode.full_refresh)) == [None]


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
    # second call was the full-object fetch: no cursor, no bounds
    assert client.queries[-1] == {"name": "account", "cursor": None, "start": None, "end": None}


def test_check_connection_invalid_tenant_returns_false():
    ok, msg = SourceZuora().check_connection(
        logger=None, config={**CONFIG, "tenant_endpoint": "Nonexistent"}
    )
    assert ok is False


def test_check_connection_success(monkeypatch):
    import source_zuora.source as src

    class OkClient:
        def __init__(self, *a, **k):
            pass
        def list_objects(self):
            return ["account", "invoice"]

    monkeypatch.setattr(src, "get_backend", lambda *a, **k: OkClient())
    ok, msg = SourceZuora().check_connection(logger=None, config=CONFIG)
    assert ok is True and msg is None


def test_check_connection_empty_objects_fails(monkeypatch):
    import source_zuora.source as src

    class EmptyClient:
        def __init__(self, *a, **k):
            pass
        def list_objects(self):
            return []

    monkeypatch.setattr(src, "get_backend", lambda *a, **k: EmptyClient())
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

    monkeypatch.setattr(src, "get_backend", lambda *a, **k: BadClient())
    ok, msg = SourceZuora().check_connection(logger=None, config=CONFIG)
    assert ok is False and msg == "bad credentials"


def test_excluded_streams_contains_archived_family():
    from source_zuora.zuora_excluded_streams import ZUORA_EXCLUDED_STREAMS

    for name in (
        "archived_guidedusage",
        "archived_prepaidbalancetransaction",
        "archived_processedusage",
        "archived_usage",
    ):
        assert name in ZUORA_EXCLUDED_STREAMS


def test_streams_filters_out_excluded_objects(monkeypatch):
    import source_zuora.source as src
    from source_zuora.zuora_excluded_streams import ZUORA_EXCLUDED_STREAMS

    discovered = ["account", "invoice", "archived_usage", "archived_guidedusage", "aggregatedataqueryslowdata"]

    class FakeClient:
        def __init__(self, *a, **k):
            pass
        def list_objects(self):
            return discovered

    monkeypatch.setattr(src, "get_backend", lambda *a, **k: FakeClient())
    names = {stream.name for stream in SourceZuora().streams(CONFIG)}
    assert names == {"account", "invoice"}
    assert names.isdisjoint(set(ZUORA_EXCLUDED_STREAMS))


def test_discover_prewarms_schemas_and_excludes(monkeypatch):
    import source_zuora.source as src

    captured = {}

    class FakeClient:
        def __init__(self, *a, **k):
            captured["client"] = self
            self.warmed = None

        def list_objects(self):
            return ["account", "invoice", "archived_usage"]

        def warm_describe_cache(self, names):
            self.warmed = list(names)

        def describe_object(self, name):
            return {"id": {"type": ["string", "null"]}, "updateddate": {"type": ["string", "null"]}}

    client = FakeClient()
    monkeypatch.setattr(src, "get_backend", lambda *a, **k: client)
    catalog = SourceZuora().discover(logger=None, config=CONFIG)

    names = sorted(s.name for s in catalog.streams)
    assert names == ["account", "invoice"]                    # archived_usage excluded
    assert captured["client"].warmed == ["account", "invoice"]  # pre-warmed, excluded filtered
    assert catalog.streams[0].json_schema["properties"]         # schema resolved into the catalog


from datetime import datetime

import pendulum

from source_zuora.source import SourceZuora, ZuoraObjectStream


class FakeBackend:
    """Records read_object calls so the stream's contract can be asserted."""

    def __init__(self, schema=None, records=None):
        # `schema or default` would treat an intentionally-empty {} as absent.
        self._schema = {"id": {"type": ["string", "null"]}} if schema is None else schema
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


BACKEND_CONFIG = {"start_date": "2026-08-01", "window_in_days": "10"}
CURSOR_SCHEMA = {
    "id": {"type": ["string", "null"]},
    "updateddate": {"type": ["string", "null"]},
}


def test_read_records_full_refresh_passes_no_bounds():
    backend = FakeBackend(records=[{"id": "a"}])
    stream = ZuoraObjectStream("account", backend, BACKEND_CONFIG)
    assert list(stream.read_records(sync_mode="full_refresh")) == [{"id": "a"}]
    assert backend.calls == [{"name": "account", "cursor": None, "start": None, "end": None}]


def test_stream_slices_yield_datetimes_not_strings():
    stream = ZuoraObjectStream("account", FakeBackend(schema=CURSOR_SCHEMA), BACKEND_CONFIG)
    first = next(iter(stream.stream_slices()))
    assert isinstance(first["start_date"], datetime)
    assert isinstance(first["end_date"], datetime)


def test_read_records_incremental_forwards_cursor_and_bounds():
    backend = FakeBackend(
        schema=CURSOR_SCHEMA,
        records=[{"id": "a", "updateddate": "2026-08-05T00:00:00+00:00"}],
    )
    stream = ZuoraObjectStream("account", backend, BACKEND_CONFIG)
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
    good = ZuoraObjectStream("account", FakeBackend(), BACKEND_CONFIG)
    empty = ZuoraObjectStream("billingpreviewrun", FakeBackend(schema={}), BACKEND_CONFIG)
    source = SourceZuora()
    monkeypatch.setattr(source, "streams", lambda config: [good, empty])
    catalog = source.discover(None, {})
    assert [s.name for s in catalog.streams] == ["account"]
