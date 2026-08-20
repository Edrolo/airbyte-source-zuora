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
    assert normalize_row("Account", {"Account.AdditionalEmailAddresses": ""}, TYPES) == {
        "additionalemailaddresses": None
    }


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


def submit_body(requests_mock):
    """The body of the POST /v1/batch-query/ call, whatever its position in history."""
    posts = [r for r in requests_mock.request_history if r.method == "POST"]
    return posts[-1].json()


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
    body = submit_body(requests_mock)
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
    assert submit_body(requests_mock)["queries"][0]["query"] == (
        "select *, ParentAccount.Id from Account "
        "where UpdatedDate >= '2026-08-01T00:00:00+00:00' "
        "and UpdatedDate <= '2026-08-01T00:00:00+00:00' "
        "order by UpdatedDate asc"
    )


def test_read_object_handles_quoted_newlines_in_csv(requests_mock):
    register_describe(requests_mock)
    register_job(
        requests_mock, ["completed"], csv_body='Account.Id,Account.Balance\n"a\nb",1\n'
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
    register_job(
        requests_mock,
        ["aborted"],
        message="java.lang.RuntimeException: You must specify a select.",
    )
    with pytest.raises(ZOQLQueryCannotProcessObject):
        list(make_aqua().read_object("account"))


def test_aborted_transiently_is_retried_then_succeeds(requests_mock):
    register_describe(requests_mock)
    requests_mock.post(f"{BASE}/v1/batch-query/", json={"id": "job-1"})
    requests_mock.get(
        f"{BASE}/v1/batch-query/jobs/job-1",
        [
            {
                "json": {
                    "status": "aborted",
                    "batches": [
                        {"status": "aborted", "message": "Service Temporarily Unavailable"}
                    ],
                }
            },
            {
                "json": {
                    "status": "completed",
                    "batches": [{"status": "completed", "fileId": "file-1"}],
                }
            },
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
    register_job(
        requests_mock, ["aborted"], message="The requested data source could not be found"
    )
    with pytest.raises(ZOQLQueryFailed, match="could not be found"):
        list(make_aqua().read_object("account"))


def test_render_query_truncates_microseconds():
    # Zuora silently matches ZERO rows when the upper bound carries fractional
    # seconds, and datetime.now() always has microseconds. Verified against a
    # sandbox: `<= '...T17:01:07.160994+10:00'` returned 0 of 3308 rows.
    start = pendulum.datetime(2026, 4, 22, 17, 1, 7, 160968, tz="Australia/Melbourne")
    end = pendulum.datetime(2026, 8, 20, 17, 1, 7, 160994, tz="Australia/Melbourne")
    query = render_query("Account", [], cursor="UpdatedDate", start=start, end=end)
    assert "." not in query
    assert query == (
        "select * from Account "
        "where UpdatedDate >= '2026-04-22T17:01:07+10:00' "
        "and UpdatedDate <= '2026-08-20T17:01:07+10:00' "
        "order by UpdatedDate asc"
    )
