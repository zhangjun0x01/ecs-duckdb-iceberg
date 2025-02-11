# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyspark.sql import SparkSession
import pyspark
import pyspark.sql

import sys
import duckdb
import os
from pyspark import SparkContext
from pathlib import Path


if (len(sys.argv) != 4 ):
    print("Usage: generate_iceberg.py <SCALE_FACTOR> <TABLE_NAME> <ICBERG_SPEC_VERSION>")
    exit(1)

SCALE = sys.argv[1]
TABLE_NAME = sys.argv[2]
ICEBERG_SPEC_VERSION = sys.argv[3]

PARQUET_SRC_FILE = f'data_generated/{TABLE_NAME}/base_file/base.parquet'
ICEBERG_TABLE_NAME = f"iceberg_catalog.{TABLE_NAME}";
CWD = os.getcwd()
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# SCALE = "0.001"
# DEST_PATH = "data/iceberg/generated_spec1_0_001"
# ICEBERG_SPEC_VERSION = "1"
# SCRIPT_DIR = "/Users/tomebergen/duckdb-iceberg/scripts/test_data_generator"

###
### Generate dataset
###
os.system(f"python3 {SCRIPT_DIR}/generate_base_parquet_irc.py {SCALE} {TABLE_NAME} spark")

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2 pyspark-shell"
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

# spark = (
#     SparkSession.builder.appName("DuckDB REST Integeration test")
#     .config(
#         "spark.sql.extensions",
#         "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
#     )
#     .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
#     .config("spark.sql.catalog.demo.type", "rest")
#     .config("spark.sql.catalog.demo.uri", "http://127.0.0.1:8181")
#     .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
#     .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/wh/")
#     .config("spark.sql.catalog.demo.s3.endpoint", "http://127.0.0.1:9000")
#     .config("spark.sql.catalog.demo.s3.path-style-access", "true")
#     .config("spark.sql.defaultCatalog", "demo")
#     .config("spark.sql.catalogImplementation", "in-memory")
#     .config('spark.jars', f'{SCRIPT_DIR}/iceberg-spark-runtime-3.5_2.12-1.4.2.jar')
#     .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
#     .getOrCreate()
# )

conf = pyspark.SparkConf()
conf.setMaster('local[*]')
conf.set('spark.sql.catalog.iceberg_catalog', 'org.apache.iceberg.spark.SparkCatalog')
conf.set('spark.sql.catalog.iceberg_catalog.type', 'hadoop')
conf.set('spark.sql.catalog.iceberg_catalog.warehouse', f"data_generated/iceberg/warehouse/default")
conf.set('spark.sql.parquet.outputTimestampType', 'TIMESTAMP_MICROS')
conf.set('spark.driver.memory', '10g')
conf.set('spark.jars', f'{SCRIPT_DIR}/iceberg-spark-runtime-3.5_2.12-1.4.2.jar')
conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

spark.read.parquet(PARQUET_SRC_FILE).createOrReplaceTempView('parquet_file_view')

if ICEBERG_SPEC_VERSION == '1':
    spark.sql(f"CREATE or REPLACE TABLE {ICEBERG_TABLE_NAME} TBLPROPERTIES ('format-version'='{ICEBERG_SPEC_VERSION}') AS SELECT * FROM parquet_file_view");
elif ICEBERG_SPEC_VERSION == '2':
    spark.sql(f"CREATE or REPLACE TABLE {ICEBERG_TABLE_NAME} TBLPROPERTIES ('format-version'='{ICEBERG_SPEC_VERSION}', 'write.update.mode'='merge-on-read') AS SELECT * FROM parquet_file_view");
else:
    print(f"Are you from the future? Iceberg spec version '{ICEBERG_SPEC_VERSION}' is unbeknownst to me")
    exit(1)

update_files = [str(path) for path in Path(f'{SCRIPT_DIR}/updates_v{ICEBERG_SPEC_VERSION}').rglob('*.sql')]
update_files.sort() # Order matters obviously
last_file = ""

file_trimmed = ""
for path in update_files:
    full_file_path = f"{SCRIPT_DIR}/updates_v{ICEBERG_SPEC_VERSION}/{os.path.basename(path)}"
    with open(full_file_path, 'r') as file:
        file_trimmed = os.path.basename(path)[:-4]
        last_file = file_trimmed
        print(f"Applying {file_trimmed} to DB")
        query = file.read()
        query = query.replace("iceberg_catalog.pyspark_iceberg_table", f"{ICEBERG_TABLE_NAME}")
        # Run spark query
        spark.sql(query)
        print(f"Writing verification data")

        # Write total count to SCRIPT_DIR/updates_vX
        ret = spark.sql(f"SELECT COUNT(*) FROM {ICEBERG_TABLE_NAME}")
        out_path = f'data_generated/{TABLE_NAME}/expected_results/{file_trimmed}/count.csv'
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, 'w') as f:
            f.write("count\n")
            f.write('%d' % ret.collect()[0][0])

        # Create copy of table
        df = spark.read.table(ICEBERG_TABLE_NAME)
        df.write.mode("overwrite").parquet(f"data_generated/{TABLE_NAME}/expected_results/{file_trimmed}/data")

###
### Finally, we copy the latest results to a "final" dir for easy test writing
###
import shutil
shutil.copytree(f"{SCRIPT_DIR}/updates_v{ICEBERG_SPEC_VERSION}/expected_results/{file_trimmed}", f"{SCRIPT_DIR}/updates_v{ICEBERG_SPEC_VERSION}/expected_results/last", dirs_exist_ok=True)