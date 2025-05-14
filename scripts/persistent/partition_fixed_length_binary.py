from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, FixedType, LongType, StringType
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform
import pyarrow as pa
import os
import glob
import duckdb

# Setup warehouse path
warehouse_path = "data/persistent/partition_fixed_length_binary"
os.makedirs(warehouse_path, exist_ok=True)

# Define fixed binary values
fixed_values = [b'hello world', b'fixed value']  # 11 bytes  # 11 bytes
user_ids = [12345, 67890]
event_types = ["click", "purchase"]

# Define schema with non-nullable fields
schema = pa.schema(
    [("partition_col", pa.binary(11), False), ("user_id", pa.int64(), False), ("event_type", pa.string(), False)]
)

arrays = [
    pa.array(fixed_values, type=pa.binary(11)),
    pa.array(user_ids, type=pa.int64()),
    pa.array(event_types, type=pa.string()),
]

# Build the RecordBatch using the schema
record_batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

# Convert to Table
table_data = pa.Table.from_batches([record_batch])

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
    NestedField(1, "partition_col", FixedType(11), required=True),
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
    identifier="default.partition_fixed_length_binary",
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

# Strip the column that we're partitioned on from the data files
parquet_files = glob.glob(
    "data/persistent/partition_fixed_length_binary/default.db/partition_fixed_length_binary/data/partition_col=*/*.parquet"
)
for file in parquet_files:
    duckdb.execute(
        f"""
        copy (
            select
                *
            EXCLUDE partition_col
            from '{file}'
        ) to '{file}'
        (
            FIELD_IDS {{
                user_id: 2, event_type: 3
            }}
        );
        """
    )
