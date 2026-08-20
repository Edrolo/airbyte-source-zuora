#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""
Manual smoke check against a real tenant. Not part of the unit suite: it needs
`secrets/config.json` and issues real AQuA export jobs.

Run: poetry run python integration_tests/aqua_smoke.py
"""

import json
import pathlib

import pendulum

from source_zuora.zuora_auth import ZuoraAuthenticator
from source_zuora.zuora_backend import AQUA, get_backend

STREAM = "account"
# Wide enough that the sandbox has updates inside it, narrow enough to be a subset.
WINDOW_DAYS = 30


def main() -> None:
    config = json.loads(pathlib.Path("secrets/config.json").read_text())
    auth = ZuoraAuthenticator(config)

    aqua = get_backend({**config, "query_api": AQUA}, auth.get_auth(), auth.url_base)
    objects = aqua.list_objects()
    print(f"AQuA objects: {len(objects)}")

    schema = aqua.describe_object(STREAM)
    foreign_keys = [key for key in schema if key.endswith("id") and key != "id"]
    print(f"{STREAM}: {len(schema)} fields, {len(foreign_keys)} FK columns: {foreign_keys}")

    full = sum(1 for _ in aqua.read_object(STREAM))
    print(f"full refresh rows: {full}")

    start = pendulum.now().subtract(days=WINDOW_DAYS)
    end = pendulum.now()
    windowed = sum(
        1 for _ in aqua.read_object(STREAM, cursor="updateddate", start=start, end=end)
    )
    print(f"last-{WINDOW_DAYS}-days rows: {windowed}")

    # The point of the check. A bad datetime literal never errors — it fails in one
    # of two silent directions, and only a real tenant reveals which:
    #   windowed == full  -> the predicate was dropped (e.g. a space-separated
    #                        literal), so every slice re-reads the whole table;
    #   windowed == 0     -> the predicate matched nothing (e.g. fractional seconds
    #                        on the upper bound), so the sync silently yields no data.
    assert windowed != full, (
        f"windowed read returned all {full} rows — the cursor predicate was "
        f"silently dropped"
    )
    assert windowed > 0, (
        f"windowed read returned 0 of {full} rows — the cursor predicate silently "
        f"matched nothing. Check the datetime literal in render_query()"
    )
    print(f"OK: incremental predicate applied ({windowed} of {full} rows)")


if __name__ == "__main__":
    main()
