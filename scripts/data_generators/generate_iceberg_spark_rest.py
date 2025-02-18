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

import os
import duckdb

in_scripts_dir = os.path.basename(os.path.dirname(__file__)) == 'scripts'
if not in_scripts_dir:
    print("please run generate_iceberg_spark_rest.py from duckdb-iceberg/scripts dir")
    exit(1)


os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2 pyspark-shell"
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

spark = (
    SparkSession.builder.appName("DuckDB REST Integeration test")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.demo.type", "rest")
    .config("spark.sql.catalog.demo.uri", "http://127.0.0.1:8181")
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/wh/")
    .config("spark.sql.catalog.demo.s3.endpoint", "http://127.0.0.1:9000")
    .config("spark.sql.catalog.demo.s3.path-style-access", "true")
    .config("spark.sql.defaultCatalog", "demo")
    .config('spark.driver.memory', '10g')
    .config("spark.sql.catalogImplementation", "in-memory")
    .config('spark.jars', f'data_generators/iceberg-spark-runtime-3.5_2.12-1.4.2.jar')
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .getOrCreate()
)

spark.sql(
    """
  CREATE DATABASE IF NOT EXISTS default;
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE default.table_unpartitioned (
    dt     date,
    number integer,
    letter string
)
USING iceberg 
"""
)


spark.sql(
    """
        INSERT INTO default.table_unpartitioned
        VALUES
            (CAST('2023-03-01' AS date), 1, 'a'),
            (CAST('2023-03-02' AS date), 2, 'b'),
            (CAST('2023-03-03' AS date), 3, 'c'),
            (CAST('2023-03-04' AS date), 4, 'd'),
            (CAST('2023-03-05' AS date), 5, 'e'),
            (CAST('2023-03-06' AS date), 6, 'f'),
            (CAST('2023-03-07' AS date), 7, 'g'),
            (CAST('2023-03-08' AS date), 8, 'h'),
            (CAST('2023-03-09' AS date), 9, 'i'),
            (CAST('2023-03-10' AS date), 10, 'j'),
            (CAST('2023-03-11' AS date), 11, 'k'),
            (CAST('2023-03-12' AS date), 12, 'l');
    """
)


spark.sql(
    """
CREATE OR REPLACE TABLE default.table_partitioned (
    dt     date,
    number integer,
    letter string
)
USING iceberg
PARTITIONED BY (days(dt))
"""
)

spark.sql(
    """
        INSERT INTO default.table_partitioned
        VALUES
            (CAST('2023-03-01' AS date), 1, 'a'),
            (CAST('2023-03-02' AS date), 2, 'b'),
            (CAST('2023-03-03' AS date), 3, 'c'),
            (CAST('2023-03-04' AS date), 4, 'd'),
            (CAST('2023-03-05' AS date), 5, 'e'),
            (CAST('2023-03-06' AS date), 6, 'f'),
            (CAST('2023-03-07' AS date), 7, 'g'),
            (CAST('2023-03-08' AS date), 8, 'h'),
            (CAST('2023-03-09' AS date), 9, 'i'),
            (CAST('2023-03-10' AS date), 10, 'j'),
            (CAST('2023-03-11' AS date), 11, 'k'),
            (CAST('2023-03-12' AS date), 12, 'l');
    """
)

# By default, Spark uses merge on write deletes
# which optimize for read-performance

spark.sql(
    """
CREATE OR REPLACE TABLE default.table_mor_deletes (
    dt     date,
    number integer,
    letter string
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
);
"""
)

spark.sql(
    """
        INSERT INTO default.table_mor_deletes
        VALUES
            (CAST('2023-03-01' AS date), 1, 'a'),
            (CAST('2023-03-02' AS date), 2, 'b'),
            (CAST('2023-03-03' AS date), 3, 'c'),
            (CAST('2023-03-04' AS date), 4, 'd'),
            (CAST('2023-03-05' AS date), 5, 'e'),
            (CAST('2023-03-06' AS date), 6, 'f'),
            (CAST('2023-03-07' AS date), 7, 'g'),
            (CAST('2023-03-08' AS date), 8, 'h'),
            (CAST('2023-03-09' AS date), 9, 'i'),
            (CAST('2023-03-10' AS date), 10, 'j'),
            (CAST('2023-03-11' AS date), 11, 'k'),
            (CAST('2023-03-12' AS date), 12, 'l');
    """
)

spark.sql(
    """
        Delete from default.table_mor_deletes
        where number > 3 and number < 10;
    """
)


#
# # TODO find better script to generate deletes in iceberg
# CWD=".."
# DEST_PATH='data/iceberg/generated_spec1_0_001'
# os.system(f"python3 test_data_generator/generate_base_parquet.py 001 {CWD}/{DEST_PATH} spark")
# location = "../data/iceberg/generated_spec1_0_001/base_file/file.parquet"
# spark.read.parquet(location).createOrReplaceTempView('parquet_lineitem_view');
#
# spark.sql(
#     """
#         CREATE OR REPLACE TABLE default.pyspark_iceberg_table
#         USING ICEBERG
#         TBLPROPERTIES (
#             'format-version'='2',
#             'write.update.mode'='merge-on-read'
#         )
#         As select * from parquet_lineitem_view
#     """
# )
#
# spark.sql("""
# update default.pyspark_iceberg_table
# set l_orderkey_bool=NULL,
#     l_partkey_int=NULL,
#     l_suppkey_long=NULL,
#     l_extendedprice_float=NULL,
#     l_extendedprice_double=NULL,
#     l_shipdate_date=NULL,
#     l_partkey_time=NULL,
#     l_commitdate_timestamp=NULL,
#     l_commitdate_timestamp_tz=NULL,
#     l_comment_string=NULL,
#     l_comment_blob=NULL
# where l_partkey_int % 2 = 0;""")
#
# spark.sql("""
# insert into default.pyspark_iceberg_table
# select * FROM default.pyspark_iceberg_table
# where l_extendedprice_double < 30000
# """)
#
# spark.sql("""
# update default.pyspark_iceberg_table
# set l_orderkey_bool = not l_orderkey_bool;
# """)
#
#
# spark.sql("""
# delete
# from default.pyspark_iceberg_table
# where l_extendedprice_double < 10000;
# """)
#
# spark.sql("""
# delete
# from default.pyspark_iceberg_table
# where l_extendedprice_double > 70000;
# """)
#
# spark.sql("""
# ALTER TABLE default.pyspark_iceberg_table
# ADD COLUMN schema_evol_added_col_1 INT DEFAULT 42;
# """)
#
# spark.sql("""
# UPDATE default.pyspark_iceberg_table
# SET schema_evol_added_col_1 = l_partkey_int
# WHERE l_partkey_int % 5 = 0;
# """)
#
# spark.sql("""
# ALTER TABLE default.pyspark_iceberg_table
# ALTER COLUMN schema_evol_added_col_1 TYPE BIGINT;
#           """)