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
            message=message,
            internal_message=message,
            failure_type=FailureType.config_error,
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
