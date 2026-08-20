#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Iterator, List, Mapping, Optional

DATA_QUERY = "Data Query"
AQUA = "AQuA"


class QueryBackend(ABC):
    """
    One Zuora extraction API. Owns its query dialect, object/field naming, and
    transport, so the streams in `source.py` deal only in lowercase names,
    `datetime` bounds, and normalized records.
    """

    @abstractmethod
    def list_objects(self) -> List[str]:
        """Queryable object names, lowercased, as stream names."""

    @abstractmethod
    def describe_object(self, name: str) -> Mapping[str, Mapping[str, Any]]:
        """Lowercase field name -> `{"type": <json schema type>}` for one object."""

    @abstractmethod
    def warm_describe_cache(self, names: List[str]) -> None:
        """Pre-populate schemas for many objects concurrently."""

    @abstractmethod
    def read_object(
        self,
        name: str,
        cursor: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> Iterator[Mapping[str, Any]]:
        """
        Records for one object. With `cursor`, `start` and `end` set, reads that
        ordered window; otherwise reads the whole object. Each backend renders the
        bounds in its own dialect — the caller passes `datetime`s.
        """


def get_backend(config: Mapping[str, Any], authenticator: Any, url_base: str) -> QueryBackend:
    """Build the query backend named by `config["query_api"]` (default Data Query)."""
    query_api = config.get("query_api") or DATA_QUERY
    if query_api == DATA_QUERY:
        from .zuora_client import ZuoraQueryClient

        return ZuoraQueryClient(
            url_base=url_base,
            authenticator=authenticator,
            data_query=config.get("data_query", "Live"),
        )
    if query_api == AQUA:
        from .zuora_aqua_client import ZuoraAquaClient

        return ZuoraAquaClient(url_base=url_base, authenticator=authenticator)
    raise ValueError(f"Unknown query_api {query_api!r}")
