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
