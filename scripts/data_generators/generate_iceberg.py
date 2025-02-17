#!/usr/bin/python3
import pyspark
import pyspark.sql
import sys
import duckdb
import os
from pyspark import SparkContext
from pathlib import Path

from scripts.data_generators.generate_base_parquet import PARQUET_SRC_FILE

if (len(sys.argv) != 4 ):
    print("Usage: generate_iceberg.py <TABLE_NAME> <ICBERG_SPEC_VERSION> <GENERATOR> <SETUP_SCRIPT> <SETUP_SCRIPT_ARGS>")
    exit(1)

TABLE_NAME = sys.argv[1]
ICEBERG_SPEC_VERSION = sys.argv[2]
GENERATOR = sys.argv[3]
SETUP_SCRIPT = sys.argv[4]
SETUP_SCRIPT_ARGS = sys.argv[5:]

TABLE_NAME = f"iceberg_catalog.{TABLE_NAME}"

DATA_GENERATION_DIR = f"./gen-data/data/{GENERATOR}/{TABLE_NAME}"
INTERMEDIATE_DATA = f"./gen-data/intermediate/{GENERATOR}/{TABLE_NAME}"

PARQUET_SRC_FILE = f"scripts/parquet_src_file.parquet"

###
### Generate data and put it in parquet file at PARQUET_SRC_FILE
###
if GENERATOR == "spark-local" and {SETUP_SCRIPT}:
    args = ""
    for arg in SETUP_SCRIPT_ARGS:
        args += str(arg) + " "
    os.system(f"python3 {SETUP_SCRIPT} {args}")
elif GENERATOR == "spark-local" and not {SETUP_SCRIPT}:
    print(f"assuming data is already in {PARQUET_SRC_FILE}")
else:
    print("Only spark Generator is supported")
    exit(1)

###
### Configure everyone's favorite apache product
###
conf = pyspark.SparkConf()
conf.setMaster('local[*]')
conf.set('spark.sql.catalog.iceberg_catalog', 'org.apache.iceberg.spark.SparkCatalog')
conf.set('spark.sql.catalog.iceberg_catalog.type', 'hadoop')
conf.set('spark.sql.catalog.iceberg_catalog.warehouse', DATA_GENERATION_DIR)
conf.set('spark.sql.parquet.outputTimestampType', 'TIMESTAMP_MICROS')
conf.set('spark.driver.memory', '10g')
conf.set('spark.jars', f'{SCRIPT_DIR}/iceberg-spark-runtime-3.5_2.12-1.4.2.jar')
conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

###
### Create Iceberg table from generated data
###
spark.read.parquet(PARQUET_SRC_FILE).createOrReplaceTempView('parquet_file_view');

###
### Apply modifications to base table generating verification results between each step
###
update_files = [str(path) for path in Path(f'scripts/data_generators/spark-local/{TABLE_NAME}/').rglob('*.sql')]
update_files.sort() # Order matters obviously
last_file = ""

for path in update_files:
    full_file_path = f"scripts/data_generators/spark-local{TABLE_NAME}/{os.path.basename(path)}"
    with open(full_file_path, 'r') as file:
        file_trimmed = os.path.basename(path)[:-4]
        last_file = file_trimmed
        print(f"Applying {file_trimmed} to DB")
        query = file.read()
        # Run spark query
        spark.sql(query)

        print(f"Writing verification data")
        # Create a parquet copy of table
        df = spark.read.table(TABLE_NAME)
        df.write.parquet(f"{INTERMEDIATE_DATA}/{file_trimmed}/data.parquet");


###
### Finally, we copy the latest results to a "final" dir for easy test writing
###
# import shutil
# shutil.copytree(f"{DEST_PATH}/expected_results/{last_file}", f"{DEST_PATH}/expected_results/last")
