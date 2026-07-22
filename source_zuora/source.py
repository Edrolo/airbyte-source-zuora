#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import logging
from datetime import datetime
from functools import cached_property
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import pendulum
from airbyte_cdk.models import AirbyteCatalog, SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import CheckpointMixin, Stream
from airbyte_cdk.utils import AirbyteTracedException

from .zuora_auth import ZuoraAuthenticator
from .zuora_client import ZuoraQueryClient
from .zuora_errors import (
    QueryWindowError,
    ZOQLQueryCannotProcessObject,
    ZOQLQueryFailed,
)
from .zuora_excluded_streams import ZUORA_EXCLUDED_STREAMS

logger = logging.getLogger("airbyte")

CURSOR_FIELD = "updateddate"
ALT_CURSOR_FIELD = "createddate"


class ZuoraObjectStream(Stream, CheckpointMixin):
    """
    One dynamically-discovered Zuora object. Emits per-stream incremental state
    keyed on the resolved cursor field.

    Uses ``CheckpointMixin`` (the ``state`` getter/setter contract) rather than the
    deprecated ``IncrementalMixin``; whether the stream syncs incrementally is decided
    by the CDK from ``cursor_field`` (empty list -> full-refresh only), not the mixin.
    """

    primary_key = "id"

    def __init__(self, name: str, client: ZuoraQueryClient, config: Mapping[str, Any]):
        self._name = name
        self._client = client
        self._config = config
        self._cursor_value: Optional[str] = None

    @property
    def name(self) -> str:
        return self._name

    @cached_property
    def cursor_field(self):
        properties = self.get_json_schema()["properties"]
        if CURSOR_FIELD in properties:
            return CURSOR_FIELD
        if ALT_CURSOR_FIELD in properties:
            return ALT_CURSOR_FIELD
        return []

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value and self.cursor_field:
            return {self.cursor_field: self._cursor_value}
        return {}

    @state.setter
    def state(self, value: Mapping[str, Any]) -> None:
        self._cursor_value = value.get(CURSOR_FIELD) or value.get(ALT_CURSOR_FIELD)

    @property
    def state_checkpoint_interval(self) -> Optional[int]:
        return None  # records within a slice are not guaranteed globally ordered

    @property
    def window_in_days(self) -> float:
        value = self._config.get("window_in_days", "90")
        try:
            return float(value)
        except (TypeError, ValueError):
            raise QueryWindowError(value)

    def get_json_schema(self) -> Mapping[str, Any]:
        return {"type": "object", "properties": dict(self._client.describe_object(self.name))}

    @staticmethod
    def _to_datetime_str(date: datetime) -> str:
        # e.g. '2021-07-15 07:45:55.000000 -07:00' — format Zuora accepts as TIMESTAMP
        return date.strftime("%Y-%m-%d %H:%M:%S.%f %Z")

    def stream_slices(
        self, *, sync_mode=SyncMode.full_refresh, cursor_field=None, stream_state=None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        if not self.cursor_field:
            yield None
            return

        local_tz = pendulum.local_timezone()
        start_date = pendulum.parse(self._config["start_date"]).in_timezone(local_tz)
        end_date = pendulum.now(local_tz)

        cursor_state = self.state.get(self.cursor_field) if self.cursor_field else None
        if cursor_state:
            start_date = pendulum.parse(cursor_state)

        start_date = min(start_date, end_date)
        while start_date <= end_date:
            end_slice = start_date.add(days=self.window_in_days)
            yield {
                "start_date": self._to_datetime_str(start_date),
                "end_date": self._to_datetime_str(end_slice),
            }
            start_date = end_slice

    def _query_incremental(self, cursor: str, stream_slice: Mapping[str, Any]) -> str:
        return (
            f"select * from {self.name} where "
            f"{cursor} >= TIMESTAMP '{stream_slice.get('start_date')}' and "
            f"{cursor} <= TIMESTAMP '{stream_slice.get('end_date')}' "
            f"order by {cursor} asc"
        )

    def _query_full(self) -> str:
        return f"select * from {self.name}"

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        cursor = self.cursor_field
        try:
            if cursor and stream_slice:
                query = self._query_incremental(cursor, stream_slice)
            else:
                query = self._query_full()
            for record in self._client.run_query(query):
                if cursor:
                    incoming = record.get(cursor)
                    if incoming:
                        self._cursor_value = max(self._cursor_value or "", incoming)
                yield record
        except ZOQLQueryCannotProcessObject as error:
            logger.warning("Skipping stream '%s': %s", self.name, error.message)
            return
        except ZOQLQueryFailed as error:
            if "cannot be resolved" not in (error.message or ""):
                raise
            # schema advertised a cursor the query engine rejected — fetch full object
            yield from self._client.run_query(self._query_full())


class SourceZuora(AbstractSource):
    def check_connection(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> Tuple[bool, Optional[Any]]:
        auth = ZuoraAuthenticator(config)
        if not auth.url_base:
            return False, (
                f"Unknown tenant_endpoint {config.get('tenant_endpoint')!r}. "
                f"Choose a valid endpoint from the connector spec."
            )
        try:
            client = ZuoraQueryClient(
                url_base=auth.url_base,
                authenticator=auth.get_auth(),
                data_query=config.get("data_query", "Live"),
            )
            objects = client.list_objects()
        except AirbyteTracedException as error:
            return False, error.message
        except Exception as error:
            return False, f"Unable to connect to Zuora: {error}"
        if not objects:
            return False, (
                "Connected to Zuora but no queryable objects were returned. Confirm the "
                "Data Query feature is enabled and the API user has query permissions."
            )
        return True, None

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        # Pre-fetch every stream's schema concurrently so the per-stream
        # get_json_schema() calls below hit the client cache. One sequential DESCRIBE
        # job per object otherwise blows past Airbyte's discover timeout on large tenants.
        streams = self.streams(config)
        if streams:
            streams[0]._client.warm_describe_cache([stream.name for stream in streams])
        return AirbyteCatalog(streams=[stream.as_airbyte_stream() for stream in streams])

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = ZuoraAuthenticator(config)
        client = ZuoraQueryClient(
            url_base=auth.url_base,
            authenticator=auth.get_auth(),
            data_query=config.get("data_query", "Live"),
        )
        return [
            ZuoraObjectStream(name, client, config)
            for name in client.list_objects()
            if name not in ZUORA_EXCLUDED_STREAMS
        ]
