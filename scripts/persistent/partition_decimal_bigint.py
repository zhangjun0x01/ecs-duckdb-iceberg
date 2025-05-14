from pyspark.sql import SparkSession
import pyspark
import pyspark.sql
from pyspark import SparkContext

import sys
import os
import glob

DATA_GENERATION_DIR = 'data/persistent'
SCRIPT_DIR = os.path.dirname(__file__)
SPARK_RUNTIME_PATH = os.path.join(SCRIPT_DIR, '..', 'data_generators', 'iceberg-spark-runtime-3.5_2.12-1.9.0.jar')

conf = pyspark.SparkConf()
conf.setMaster('local[*]')
conf.set('spark.sql.catalog.iceberg_catalog', 'org.apache.iceberg.spark.SparkCatalog')
conf.set('spark.sql.catalog.iceberg_catalog.type', 'hadoop')
conf.set('spark.sql.catalog.iceberg_catalog.warehouse', DATA_GENERATION_DIR)
conf.set('spark.sql.parquet.outputTimestampType', 'TIMESTAMP_MICROS')
conf.set('spark.driver.memory', '10g')
conf.set('spark.jars', SPARK_RUNTIME_PATH)
conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
spark.sql("USE iceberg_catalog")

import pathlib
import tempfile
import duckdb

spark.sql(
    """
CREATE OR REPLACE TABLE partition_decimal_bigint (
    partition_col DECIMAL(16,11),
    user_id BIGINT,
    event_type STRING
)
USING iceberg
PARTITIONED BY (partition_col)
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read',
    'write.data.partition-columns' = false,
    'write.parquet.write-partition-values' = false
);
"""
)

spark.sql(
    """
INSERT INTO partition_decimal_bigint VALUES
  (42.56378901234, 12345, 'click'),
  (1.23456789012, 67890, 'purchase'),
  (1234.54321098765, 111213, 'delete'),
  (-9876.54321098765, 222333, 'view'),
  (0.00000000001, 444555, 'scroll'),
  (9999.99999999999, 666777, 'hover'),
  (-0.12345678901, 888999, 'drag'),
  (123.45678901234, 123456, 'drop'),
  (-5432.10987654321, 654321, 'swipe'),
  (0.00000000000, 987654, 'pinch');
"""
)

# Strip the column that we're partitioned on from the data files
parquet_files = glob.glob("data/persistent/partition_decimal_bigint/data/partition_col=*/*.parquet")
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
