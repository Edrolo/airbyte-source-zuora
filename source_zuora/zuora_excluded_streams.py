#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#


from typing import List

"""
Zuora object (API table) names that should NOT be discovered as streams — either
because they are service/aggregate objects that hold no queryable data, or because
they persistently fail to be queried on the tenant. Extend this list if needed.

The `archived_*` objects belong to Zuora's Archived Data store. On tenants where
that store is not provisioned, every `DESCRIBE`/`SELECT` against them returns
"Service Temporarily Unavailable" (error code LINK_30000007). Because the connector
treats that message as a *transient* error, it would otherwise retry them and then
fail the whole sync rather than skip them — so they are excluded explicitly.
(Verified empirically: on an APAC sandbox, these four were the only objects out of
187 that failed `DESCRIBE`, and they failed persistently across retries.)
"""

ZUORA_EXCLUDED_STREAMS: List = [
    "aggregatedataqueryslowdata",
    "archived_guidedusage",
    "archived_prepaidbalancetransaction",
    "archived_processedusage",
    "archived_usage",
]
