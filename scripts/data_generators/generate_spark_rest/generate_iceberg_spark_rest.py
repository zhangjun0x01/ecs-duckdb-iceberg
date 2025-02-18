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

#!/usr/bin/python3
import pyspark
import pyspark.sql
import sys
import duckdb
import os
from pyspark import SparkContext
from pathlib import Path
import duckdb
import pdb


DATA_GENERATION_DIR = f"./gen-data/data/spark-rest/"
SCRIPT_DIR = f"./scripts/data_generators/"

class IcebergSparkRest():
    def __init__(self):
        pass

    ###
    ### Configure everyone's favorite apache product
    ###
    def GetConnection(self):
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
            .config('spark.jars', f'{SCRIPT_DIR}/iceberg-spark-runtime-3.5_2.12-1.4.2.jar')
            .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
            .getOrCreate()
        )

        spark.sql(
            """
          CREATE DATABASE IF NOT EXISTS default;
        """
        )
        return spark

    def GetSQLFiles(self, table_dir):
        sql_files = [f for f in os.listdir(table_dir) if f.endswith('.sql')]  # Find .sql files
        sql_files.sort() # Order matters obviously # Store results
        return sql_files

    def GetTableDirs(self):
        dir = "./scripts/data_generators/generate_spark_rest/"
        subdirectories = [d for d in os.listdir(dir) if os.path.isdir(dir + d) and d != "__pycache__"]
        return subdirectories

    def GetSetupFile(self, dir):
        setup_files = [f for f in os.listdir(dir) if 'setup' in f.lower() and os.path.isfile(f)]
        if len(setup_files) == 0:
            return ""
        return setup_files[0]

    def GenerateTables(self, con):
        # con is spark_session
        # first get the sub_directories in the current directory
        for table_dir in self.GetTableDirs():
            full_table_dir = f"./scripts/data_generators/generate_spark_rest/{table_dir}"
            setup_script = self.GetSetupFile(full_table_dir)
            if setup_script != "":
                os.system(f"python3 {full_table_dir}/{os.path.basename(setup_script)}")

            # should mimic generate_base_parquet
            duckdb_con = duckdb.connect()
            duckdb_con.execute("call dbgen(sf=1)")
            duckdb_con.query("""CREATE VIEW test_table as
                    SELECT
                    (l_orderkey%2=0) as l_orderkey_bool,
                    l_partkey::INT32 as l_partkey_int,
                    l_suppkey::INT64 as l_suppkey_long,
                    l_extendedprice::FLOAT as l_extendedprice_float,
                    l_extendedprice::DOUBLE as l_extendedprice_double,
                    l_extendedprice::DECIMAL(9,2) as l_extendedprice_dec9_2,
                    l_extendedprice::DECIMAL(18,6) as l_extendedprice_dec18_6,
                    l_extendedprice::DECIMAL(38,10) as l_extendedprice_dec38_10,
                    l_shipdate::DATE as l_shipdate_date,
                    l_partkey as l_partkey_time,
                    l_commitdate::TIMESTAMP as l_commitdate_timestamp,
                    l_commitdate::TIMESTAMPTZ as l_commitdate_timestamp_tz,
                    l_comment as l_comment_string,
                    gen_random_uuid()::VARCHAR as uuid,
                    l_comment::BLOB as l_comment_blob
                    FROM
                    lineitem;""")

            PARQUET_SRC_FILE = "scripts/data_generators/generate_spark_rest/lineitem.parquet"
            duckdb_con.execute(f"copy test_table to '{PARQUET_SRC_FILE}' (FORMAT PARQUET)")

            # TODO fix this
            con.read.parquet(PARQUET_SRC_FILE).createOrReplaceTempView('parquet_file_view')
            update_files = self.GetSQLFiles(full_table_dir)

            for path in update_files:
                full_file_path = f"{full_table_dir}/{os.path.basename(path)}"
                with open(full_file_path, 'r') as file:
                    file_trimmed = os.path.basename(path)[:-4]
                    last_file = file_trimmed
                    query = file.read()
                    # Run spark query
                    con.sql(query)

                    # # Create a parquet copy of table
                    # df = spark.read.table(TABLE_NAME)
                    # df.write.parquet(f"{INTERMEDIATE_DATA}/{file_trimmed}/data.parquet");

    def CloseConnection(self, con):
        pass

