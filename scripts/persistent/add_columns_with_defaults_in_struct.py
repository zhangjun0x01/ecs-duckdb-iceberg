from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    DateType,
    TimeType,
    TimestampType,
    TimestamptzType,
    StringType,
    UUIDType,
    FixedType,
    BinaryType,
    NestedField,
    StructType,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.table.metadata import TableMetadata
from datetime import time, datetime, timezone
import pyarrow as pa
import os
import glob
import duckdb

# Setup warehouse path
warehouse_path = "data/persistent/add_columns_with_defaults_in_struct"
os.makedirs(warehouse_path, exist_ok=True)

# Load SQLite-backed local catalog
catalog = load_catalog(
    "default",
    **{
        "type": "sql",
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": warehouse_path,
    },
)

# Create the namespace if it doesn't exist
try:
    catalog.create_namespace("default")
except Exception as e:
    print(f"Note: {e}")  # Print but continue if namespace already exists

# Define schema with NestedField objects
schema = Schema(
    NestedField(
        9,
        'a',
        field_type=StructType(NestedField(field_id=3, name="col1", field_type=StringType(), required=False)),
        required=False,
    )
)

# Create table
table = catalog.create_table(
    identifier="default.add_columns_with_defaults_in_struct",
    schema=schema,
    properties={"format-version": "2", "write.update.mode": "merge-on-read"},
)

# Write records
table_data = duckdb.sql(
    """
    select {
        'col1': 'test'::VARCHAR
    } a
"""
).arrow()
table.append(table_data)

with table.update_schema() as update:
    print(update.__class__)
    update.add_column(('a', "col_boolean"), BooleanType(), default_value=True, required=False)
    update.add_column(('a', "col_integer"), IntegerType(), default_value=342342, required=False)
    update.add_column(('a', "col_long"), LongType(), default_value=-9223372036854775808, required=False)
    update.add_column(('a', "col_float"), FloatType(), default_value=0.34234, required=False)
    update.add_column(('a', "col_double"), DoubleType(), default_value=0.342343242342342, required=False)
    update.add_column(('a', "col_decimal"), DecimalType(16, 2), default_value=12345, required=False)
    update.add_column(('a', "col_date"), DateType(), default_value=12345, required=False)
    update.add_column(('a', "col_time"), TimeType(), default_value=12345, required=False)
    update.add_column(('a', "col_timestamp"), TimestampType(), default_value=12345, required=False)
    update.add_column(('a', "col_timestamptz"), TimestamptzType(), default_value=12345, required=False)
    update.add_column(('a', "col_string"), StringType(), default_value="HELLO", required=False)
    update.add_column(
        ('a', "col_uuid"), UUIDType(), default_value="f79c3e09-677c-4bbd-a479-3f349cb785e7", required=False
    )
    update.add_column(('a', "col_fixed"), FixedType(5), default_value=b"\x01\x02\x03\xff\x03", required=False)
    update.add_column(('a', "col_binary"), BinaryType(), default_value=b"\x01\x02", required=False)

# Write records
query = r"""
    select {
        'col1': 'test',
        'col_boolean': 'false'::BOOLEAN,
        'col_integer': '453243'::INTEGER,
        'col_long': '328725092345834'::BIGINT,
        'col_float': '23.34342'::FLOAT,
        'col_double': '23.343424523423434'::DOUBLE,
        'col_decimal': '3423434.23'::DECIMAL(16,2),
        'col_date': '11-03-05'::DATE,
        'col_time': '12:06:45'::TIME,
        'col_timestamp': '11-03-05 12:06:45'::TIMESTAMP,
        'col_string': 'World',
        'col_binary': '\x80\x00\x80'::BLOB,
    } a
"""
table_data = duckdb.sql(query).arrow()
# table_data = table_data.set_column(
#    table_data.schema.get_field_index('col_timestamptz'),
#    'col_timestamptz',
#    pa.array([datetime(2023, 5, 15, 14, 30, 45, tzinfo=timezone.utc)]),
# )

from uuid import UUID

# new_col_uuid = pa.array([UUID('020d4fc7-acd6-45ac-b216-7873f4038e1f').bytes], pa.binary(16))
# new_col_fixed = pa.array([b'\x80\x00\x80\x00\x80'], pa.binary(5))

# table_data = table_data.add_column(13, 'col_uuid', new_col_uuid)
# table_data = table_data.add_column(12, 'col_fixed', new_col_fixed)

table.append(table_data)
