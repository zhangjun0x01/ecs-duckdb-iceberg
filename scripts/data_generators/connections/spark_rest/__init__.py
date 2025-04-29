#!/usr/bin/python3

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
from pyspark import SparkContext

from . import IcebergConnection

import sys
import os

CONNECTION_KEY = 'spark-rest'

SPARK_RUNTIME_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'iceberg-spark-runtime-3.5_2.12-1.4.2.jar')


@IcebergConnection.register(CONNECTION_KEY)
class IcebergSparkRest(IcebergConnection):
    def __init__(self):
        super().__init__(CONNECTION_KEY)
        self.con = get_connection(self)

    def get_connection(self):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2 pyspark-shell"
        )
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_ACCESS_KEY_ID"] = "admin"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

        spark = (
            SparkSession.builder.appName("DuckDB REST Integration test")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.demo.type", "rest")
            .config("spark.sql.catalog.demo.uri", "http://127.0.0.1:8181")
            .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/wh/")
            .config("spark.sql.catalog.demo.s3.endpoint", "http://127.0.0.1:9000")
            .config("spark.sql.catalog.demo.s3.path-style-access", "true")
            .config("spark.sql.defaultCatalog", "demo")
            .config('spark.driver.memory', '10g')
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config('spark.jars', SPARK_RUNTIME_PATH)
            .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
            .getOrCreate()
        )
        spark.sql("CREATE DATABASE IF NOT EXISTS default;")
        return spark

    # def GenerateTPCH(self, con):
    #    duckdb_con = duckdb.connect()
    #    duckdb_con.execute("call dbgen(sf=1)")

    #    for tbl in ['lineitem', 'customer', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']:
    #        create_statement = f"""
    #            CREATE or REPLACE TABLE default.{tbl}_sf1
    #            TBLPROPERTIES (
    #                'format-version'='2',
    #                'write.update.mode'='merge-on-read'
    #            )
    #            AS SELECT * FROM parquet_file_view;
    #        """
    #        duckdb_con.execute(f"copy {tbl} to '{PARQUET_SRC_FILE}' (FORMAT PARQUET)")
    #        con.read.parquet(PARQUET_SRC_FILE).createOrReplaceTempView('parquet_file_view')
    #        con.sql(create_statement)
