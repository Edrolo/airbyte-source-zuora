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
