#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""
Parsers for Zuora's Describe API (`GET /v1/describe`, `GET /v1/describe/{object}`),
which returns `text/xml` with no JSON alternative. Pure functions over bytes so the
correctness-critical field and relationship handling is directly testable.
"""

import xml.etree.ElementTree as ET
from typing import Dict, Iterable, List, Mapping

# Only fields whose <contexts> include this are selectable in an Export ZOQL query.
EXPORT_CONTEXT = "export"


def parse_object_names(xml: bytes) -> List[str]:
    """Canonical object names from `GET /v1/describe`."""
    root = ET.fromstring(xml)
    return [name for name in (o.findtext("name") for o in root.findall("object")) if name]


def parse_fields(xml: bytes) -> Dict[str, str]:
    """
    Export-context fields from `GET /v1/describe/{object}`, as canonical field
    name -> Zuora type. Fields lacking the `export` context exist in the object
    model but fail an Export ZOQL query, so they are dropped.
    """
    root = ET.fromstring(xml)
    fields = {}
    for field in root.findall("fields/field"):
        name = field.findtext("name")
        if not name:
            continue
        contexts = {c.text for c in field.findall("contexts/context")}
        if EXPORT_CONTEXT in contexts:
            fields[name] = field.findtext("type") or ""
    return fields


def parse_relationships(xml: bytes) -> List[str]:
    """
    Relationship names from the `<related-objects>` section. Foreign keys are not
    plain columns in Export ZOQL; they are selected as `<Relationship>.Id`. An
    unknown relationship name is a submit-time error, so these are never guessed.
    """
    root = ET.fromstring(xml)
    return [
        name
        for name in (o.findtext("name") for o in root.findall("related-objects/object"))
        if name
    ]


def foreign_key_columns(
    fields: Mapping[str, str], relationships: Iterable[str]
) -> Dict[str, str]:
    """
    Map each usable relationship to the column name its `<Rel>.Id` selection will
    produce, matching Data Query's naming (`Account` -> `accountid`).

    Relationships whose derived name is already an own field are skipped: some
    objects own e.g. `SubscriptionId` *and* have a `Subscription` relationship, and
    selecting both would emit a duplicate column.
    """
    own = {name.lower() for name in fields}
    columns: Dict[str, str] = {}
    for relationship in relationships:
        column = f"{relationship.lower()}id"
        if column in own or column in columns.values():
            continue
        columns[relationship] = column
    return columns
