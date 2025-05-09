from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, TimestamptzType, LongType, StringType
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform
from pyiceberg.utils.datetime import datetime_to_micros
from datetime import datetime, timezone
import pyarrow as pa
import os

timestamps = [
    datetime(2023, 5, 15, 14, 30, 45, tzinfo=timezone.utc),
    datetime(2023, 8, 22, 9, 15, 20, tzinfo=timezone.utc),
]
# Convert to microseconds since epoch
micros = [datetime_to_micros(ts) for ts in timestamps]
user_ids = [12345, 67890]
event_types = ["click", "purchase"]

# Define schema with non-nullable fields
schema = pa.schema(
    [
        ("partition_col", pa.timestamp("us", tz="UTC"), False),
        ("user_id", pa.int64(), False),
        ("event_type", pa.string(), False),
    ]
)

arrays = [
    pa.array(timestamps, type=pa.timestamp("us", tz="UTC")),
    pa.array(user_ids, type=pa.int64()),
    pa.array(event_types, type=pa.string()),
]

# Build the RecordBatch using the schema
record_batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

# Convert to Table
table_data = pa.Table.from_batches([record_batch])

# Setup warehouse path
warehouse_path = "data/persistent/partition_timestamptz"
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
    NestedField(1, "partition_col", TimestamptzType(), required=True),
    NestedField(2, "user_id", LongType(), required=True),
    NestedField(3, "event_type", StringType(), required=True),
)

# Define partition spec
partition_spec = PartitionSpec(
    PartitionField(
        source_id=schema.find_field("partition_col").field_id,
        field_id=1000,
        transform=IdentityTransform(),
        name="partition_col",
    )
)

# Create table
table = catalog.create_table(
    identifier="default.partition_timestamptz",
    schema=schema,
    partition_spec=partition_spec,
    properties={
        "format-version": "2",
        "write.update.mode": "merge-on-read",
        "write.data.partition-columns": "false",
        "write.parquet.write-partition-values": "false",
    },
)

# Write records
table.append(table_data)
