#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

from typing import Dict, List, Optional

TYPE_NUMBER = ["number", "null"]
TYPE_STRING = ["string", "null"]
TYPE_OBJECT = ["object", "null"]
TYPE_ARRAY = ["array", "null"]
TYPE_BOOL = ["boolean", "null"]

# Keys are lowercase; look them up through json_type(), which casefolds. The Data
# Query `DESCRIBE` output and the AQuA Describe API disagree on case (the latter
# returns e.g. "ZOQL"), so a case-sensitive lookup silently degrades types to string.
TYPE_MAPPING: Dict[str, List[str]] = {
    "decimal(22,9)": TYPE_NUMBER,
    "decimal": TYPE_NUMBER,
    "integer": TYPE_NUMBER,
    "int": TYPE_NUMBER,
    "bigint": TYPE_NUMBER,
    "smallint": TYPE_NUMBER,
    "double": TYPE_NUMBER,
    "float": TYPE_NUMBER,
    "number": TYPE_NUMBER,
    "timestamp": TYPE_NUMBER,
    "date": TYPE_STRING,
    "datetime": TYPE_STRING,
    "timestamp with time zone": TYPE_STRING,
    "picklist": TYPE_STRING,
    "text": TYPE_STRING,
    "varchar": TYPE_STRING,
    "zoql": TYPE_OBJECT,
    "binary": TYPE_OBJECT,
    "json": TYPE_OBJECT,
    "xml": TYPE_OBJECT,
    "blob": TYPE_OBJECT,
    "list": TYPE_ARRAY,
    "array": TYPE_ARRAY,
    "boolean": TYPE_BOOL,
    "bool": TYPE_BOOL,
}


def json_type(zuora_type: Optional[str]) -> List[str]:
    """Map a Zuora column/field type onto a JSON-schema type, defaulting to string."""
    return TYPE_MAPPING.get((zuora_type or "").strip().lower(), TYPE_STRING)
