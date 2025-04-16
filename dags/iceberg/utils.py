import logging

from pyiceberg.schema import Schema

logger = logging.getLogger(__name__)


def merge_schemas(schema1: Schema, schema2: Schema):
    """
    Merge two Iceberg schemas into a single schema.
    """
    appended_fields = list(schema1.fields) + list(schema2.fields)
    return Schema(*appended_fields)
