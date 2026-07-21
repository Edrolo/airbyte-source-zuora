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


class ZOQLQueryFailed(Exception):
    """A ZOQL Data Query job terminated in a non-completed state."""

    def __init__(self, message: str, query: str = ""):
        self.message = message
        self.query = query
        super().__init__(f"{message}, QUERY: {query}")


class ZOQLQueryCannotProcessObject(Exception):
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
        self.message = message
        super().__init__(message)
