"""
Test scaffolding for the in-progress airbyte-cdk 7.x migration.

`source_zuora/__init__.py` eagerly imports `.source`, which (along with
`zuora_auth.py`) has not been migrated yet and still references APIs removed
from airbyte-cdk 7.x (e.g. `AirbyteLogger`). Until those files are migrated in
a later task, a plain `import source_zuora.<submodule>` would fail during
package initialization even for submodules (like `zuora_errors`) that have
already been fully modernized and are independently testable.

To keep unit tests for already-migrated submodules unblocked, register a
lightweight stand-in for the `source_zuora` package in `sys.modules` before
any test imports run. This stand-in points `__path__` at the real package
directory but does not execute `source_zuora/__init__.py`, so
`from source_zuora.zuora_errors import ...` resolves `zuora_errors.py`
directly without pulling in the not-yet-migrated modules.

This file can be deleted once `source.py` / `zuora_auth.py` are migrated and
`source_zuora/__init__.py` imports cleanly again.
"""

import pathlib
import sys
import types

if "source_zuora" not in sys.modules:
    _package_dir = pathlib.Path(__file__).parent.parent / "source_zuora"
    _stub = types.ModuleType("source_zuora")
    _stub.__path__ = [str(_package_dir)]
    sys.modules["source_zuora"] = _stub
