from pyspark.sql import SparkSession
import pyspark
import pyspark.sql
from pyspark import SparkContext
from datetime import datetime

import sys
import os
import glob

DATA_GENERATION_DIR = 'data/persistent'
SCRIPT_DIR = os.path.dirname(__file__)
SPARK_RUNTIME_PATH = os.path.join(SCRIPT_DIR, '..', 'data_generators', 'iceberg-spark-runtime-3.5_2.12-1.4.2.jar')

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

# Create a DataFrame with timestamp data from different years
timestamp_data = [
    (datetime(2020, 5, 15, 14, 30, 45), 12345, "click"),
    (datetime(2021, 8, 22, 9, 15, 20), 67890, "purchase"),
    (datetime(2022, 3, 10, 11, 45, 30), 54321, "view"),
]

# Create a DataFrame with the timestamp data
df = spark.createDataFrame(timestamp_data, ["partition_col", "user_id", "event_type"])

# Register the DataFrame as a temporary view
df.createOrReplaceTempView("timestamp_data")

spark.sql(
    """
CREATE OR REPLACE TABLE partition_timestamp_year (
    partition_col TIMESTAMP_NTZ,
    user_id BIGINT,
    event_type STRING
)
USING iceberg
PARTITIONED BY (year(partition_col))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read',
    'write.data.partition-columns' = false,
    'write.parquet.write-partition-values' = false
);
"""
)

# Insert the data from the temporary view
spark.sql(
    """
INSERT INTO partition_timestamp_year
SELECT * FROM timestamp_data;
"""
)
