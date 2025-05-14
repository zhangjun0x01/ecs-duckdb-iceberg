from pyspark.sql import SparkSession
import pyspark
import pyspark.sql
from pyspark import SparkContext

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

# Generate tpch 0.01

import pathlib
import tempfile
import duckdb


spark.sql(
    """
CREATE OR REPLACE TABLE hive_partitioned_table (
    event_date DATE,
    user_id BIGINT,
    event_type STRING
)
USING iceberg
PARTITIONED BY (event_date)
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
INSERT INTO hive_partitioned_table VALUES
  (DATE'2024-01-01', 12345, 'click'),
  (DATE'2024-01-02', 67890, 'purchase');
"""
)

spark.sql(
    """
ALTER TABLE hive_partitioned_table 
ADD PARTITION FIELD event_type
"""
)

# Refresh the table to ensure the new partition spec is used
spark.sql("REFRESH TABLE hive_partitioned_table")

# Now insert new data that will be partitioned by both event_date and event_type
spark.sql(
    """
INSERT INTO hive_partitioned_table VALUES
  (DATE'2024-01-03', 13579, 'view'),
  (DATE'2024-01-03', 24680, 'click'),
  (DATE'2024-01-04', 97531, 'purchase'),
  (DATE'2024-01-04', 86420, 'view');
"""
)

# Strip the column that we're partitioned on from the data files
parquet_files = glob.glob("data/persistent/hive_partitioned_table/data/event_date=2024-01-0*/*.parquet")
for file in parquet_files:
    duckdb.execute(
        f"""
		copy (
			select
				*
			EXCLUDE event_date
			from '{file}'
		) to '{file}'
		(
			FIELD_IDS {{
				user_id: 2, event_type: 3
			}}
		);
	"""
    )

parquet_files = glob.glob("data/persistent/hive_partitioned_table/data/event_date=2024-01-0*/event_type=*/*.parquet")
for file in parquet_files:
    duckdb.execute(
        f"""
		copy (
			select
				*
			EXCLUDE (event_date, event_type)
			from '{file}'
		) to '{file}'
		(
			FIELD_IDS {{
				user_id: 2
			}}
		);
	"""
    )
