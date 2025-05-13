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

# Define schema with non-nullable fields
schema = pa.schema([("col1", pa.string(), False)])
col1_data = ["click", "purchase"]
arrays = [
    pa.array(col1_data, type=pa.string()),
]
record_batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
table_data = pa.Table.from_batches([record_batch])

# Setup warehouse path
warehouse_path = "data/persistent/add_columns_with_defaults"
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
    NestedField(3, "col1", StringType(), required=False),
)

# Create table
table = catalog.create_table(
    identifier="default.add_columns_with_defaults",
    schema=schema,
    properties={"format-version": "2", "write.update.mode": "merge-on-read"},
)

# Write records
table.append(table_data)

with table.update_schema() as update:
    print(update.__class__)
    update.add_column("col_boolean", BooleanType(), default_value=True, required=False)
    update.add_column("col_integer", IntegerType(), default_value=342342, required=False)
    update.add_column("col_long", LongType(), default_value=-9223372036854775808, required=False)
    update.add_column("col_float", FloatType(), default_value=0.34234, required=False)
    update.add_column("col_double", DoubleType(), default_value=0.342343242342342, required=False)
    update.add_column("col_decimal", DecimalType(16, 2), default_value=12345, required=False)
    update.add_column("col_date", DateType(), default_value=12345, required=False)
    update.add_column("col_time", TimeType(), default_value=12345, required=False)
    update.add_column("col_timestamp", TimestampType(), default_value=12345, required=False)
    update.add_column("col_timestamptz", TimestamptzType(), default_value=12345, required=False)
    update.add_column("col_string", StringType(), default_value="HELLO", required=False)
    update.add_column("col_uuid", UUIDType(), default_value="f79c3e09-677c-4bbd-a479-3f349cb785e7", required=False)
    update.add_column("col_fixed", FixedType(5), default_value=b"\x01\x02\x03\xff\x03", required=False)
    update.add_column("col_binary", BinaryType(), default_value=b"\x01\x02", required=False)

# Write records
query = r"""
	select
		'test' "col1",
		'false'::BOOLEAN "col_boolean",
		'453243'::INTEGER "col_integer",
		'328725092345834'::BIGINT "col_long",
		'23.34342'::FLOAT "col_float",
		'23.343424523423434'::DOUBLE "col_double",
		'3423434.23'::DECIMAL(16,2) "col_decimal",
		'11-03-05'::DATE "col_date",
		'12:06:45'::TIME "col_time",
		'11-03-05 12:06:45'::TIMESTAMP "col_timestamp",
		'11-03-05 12:06:45'::TIMESTAMPTZ "col_timestamptz",
		'World' "col_string",
		'\x80\x00\x80\x00\x80'::BLOB "col_fixed",
		'\x80\x00\x80'::BLOB "col_binary"
"""
table_data = duckdb.sql(query).arrow()
table_data = table_data.set_column(
    table_data.schema.get_field_index('col_timestamptz'),
    'col_timestamptz',
    pa.array([datetime(2023, 5, 15, 14, 30, 45, tzinfo=timezone.utc)]),
)

from uuid import UUID

# Replacement for col_uuid
new_col_uuid = pa.array([UUID('020d4fc7-acd6-45ac-b216-7873f4038e1f').bytes], pa.binary(16))

# Replacement for col_fixed (must be exactly 5 bytes per element)
new_col_fixed = pa.array([b'\x80\x00\x80\x00\x80'], pa.binary(5))

table_data = table_data.add_column(13, 'col_uuid', new_col_uuid)

table_data = table_data.set_column(table_data.schema.get_field_index('col_fixed'), 'col_fixed', new_col_fixed)

table.append(table_data)
