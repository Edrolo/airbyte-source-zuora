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
