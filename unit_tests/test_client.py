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


def test_describe_object_is_memoized(requests_mock):
    register_job(requests_mock, ["completed"])
    requests_mock.get(
        "https://s3/result.jsonl",
        text='{"Column": "id", "Type": "varchar"}\n',
    )
    client = make_client()
    first = client.describe_object("account")
    call_count_after_first = requests_mock.call_count
    second = client.describe_object("account")
    assert second == first
    # no additional HTTP calls were made for the second describe
    assert requests_mock.call_count == call_count_after_first


def test_poll_job_times_out(requests_mock):
    requests_mock.post(f"{BASE}/query/jobs", json={"data": {"id": "job-1"}})
    requests_mock.get(
        f"{BASE}/query/jobs/job-1",
        json={"data": {"queryStatus": "in_progress", "query": "select 1"}},
    )
    client = ZuoraQueryClient(BASE, FakeAuth(), poll_interval=0, max_poll_attempts=2)
    with pytest.raises(ZOQLQueryFailed):
        list(client.run_query("select * from account"))


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


def test_request_retries_connection_error_then_succeeds(requests_mock):
    import requests
    requests_mock.post(
        f"{BASE}/query/jobs",
        [
            {"exc": requests.exceptions.ConnectionError},
            {"json": {"data": {"id": "job-1"}}, "status_code": 200},
        ],
    )
    requests_mock.get(
        f"{BASE}/query/jobs/job-1",
        json={"data": {"queryStatus": "completed", "dataFile": "https://s3/r.jsonl"}},
    )
    requests_mock.get("https://s3/r.jsonl", text='{"id": "a"}\n')
    client = ZuoraQueryClient(BASE, FakeAuth(), poll_interval=0, backoff_factor=0)
    assert list(client.run_query("select 1")) == [{"id": "a"}]


def test_request_raises_transient_on_persistent_connection_error(requests_mock):
    import requests
    from source_zuora.zuora_errors import ZuoraTransientError

    requests_mock.post(f"{BASE}/query/jobs", exc=requests.exceptions.ConnectTimeout)
    client = ZuoraQueryClient(BASE, FakeAuth(), poll_interval=0, backoff_factor=0, max_retries=2)
    with pytest.raises(ZuoraTransientError):
        list(client.run_query("select 1"))
