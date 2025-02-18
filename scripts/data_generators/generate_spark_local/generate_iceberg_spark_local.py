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


# import DataGenerationBase

# from scripts.data_generators.generate_base_parquet import PARQUET_SRC_FILE

DATA_GENERATION_DIR = f"./gen-data/data/spark-local/"
SCRIPT_DIR = f"./scripts/data_generators/"

class IcebergSparkLocal():
    def __init__(self):
        pass

    ###
    ### Configure everyone's favorite apache product
    ###
    def GetConnection(self):
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
        return spark

    def GetSQLFiles(self, table_dir):
        sql_files = [f for f in os.listdir(table_dir) if f.endswith('.sql')]  # Find .sql files
        sql_files.sort() # Order matters obviously # Store results
        return sql_files

    def GetTableDirs(self):
        dir = "./scripts/data_generators/generate_spark_local/"
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
            full_table_dir = f"./scripts/data_generators/generate_spark_local/{table_dir}"
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

            PARQUET_SRC_FILE = "scripts/data_generators/generate_spark_local/lineitem.parquet"
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




# if (len(sys.argv) != 4 ):
#     print("Usage: generate_iceberg_spark_local.py <TABLE_NAME> <ICBERG_SPEC_VERSION> <GENERATOR> <SETUP_SCRIPT> <SETUP_SCRIPT_ARGS>")
#     exit(1)
#
# TABLE_NAME = sys.argv[1]
# ICEBERG_SPEC_VERSION = sys.argv[2]
# GENERATOR = sys.argv[3]
# SETUP_SCRIPT = sys.argv[4]
# SETUP_SCRIPT_ARGS = sys.argv[5:]
#
# TABLE_NAME = f"iceberg_catalog.{TABLE_NAME}"
#

# INTERMEDIATE_DATA = f"./gen-data/intermediate/{GENERATOR}/{TABLE_NAME}"
#
# PARQUET_SRC_FILE = f"scripts/parquet_src_file.parquet"



###
### Create Iceberg table from generated data
###

###
### Finally, we copy the latest results to a "final" dir for easy test writing
###
# import shutil
# shutil.copytree(f"{DEST_PATH}/expected_results/{last_file}", f"{DEST_PATH}/expected_results/last")
