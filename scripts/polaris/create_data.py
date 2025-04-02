import os
from pyspark.sql import SparkSession

client_id = os.getenv('POLARIS_CLIENT_ID', '')
client_secret = os.getenv('POLARIS_SECRET_ID', '')

spark = (SparkSession.builder
         .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
         .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.hadoop:hadoop-aws:3.4.0,software.amazon.awssdk:bundle:2.23.19,software.amazon.awssdk:url-connection-client:2.23.19")
         .config('spark.sql.iceberg.vectorization.enabled', 'false')
         # Configure the 'polaris' catalog as an Iceberg rest catalog
         .config("spark.sql.catalog.polaris.type", "rest")
         .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
         # Specify the rest catalog endpoint
         .config("spark.sql.catalog.polaris.uri", "http://polaris:8181/api/catalog")
         # Enable token refresh
         .config("spark.sql.catalog.polaris.token-refresh-enabled", "true")
         # specify the client_id:client_secret pair
         .config("spark.sql.catalog.polaris.credential", f"{client_id}:{client_secret}")
         # Set the warehouse to the name of the catalog we created
         .config("spark.sql.catalog.polaris.warehouse", catalog_name)
         # Scope set to PRINCIPAL_ROLE:ALL
         .config("spark.sql.catalog.polaris.scope", 'PRINCIPAL_ROLE:ALL')
         # Enable access credential delegation
         .config("spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation", 'vended-credentials')
         .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.io.ResolvingFileIO")
         .config("spark.sql.catalog.polaris.s3.region", "us-west-2")
         .config("spark.history.fs.logDirectory", "/home/iceberg/spark-events")).getOrCreate()


spark.sql("USE polaris")
spark.sql("SHOW NAMESPACES").show()

spark.sql("CREATE NAMESPACE IF NOT EXISTS COLLADO_TEST")
spark.sql("USE NAMESPACE COLLADO_TEST")
spark.sql("""CREATE TABLE IF NOT EXISTS TEST_TABLE (
    id bigint NOT NULL COMMENT 'unique id',
    data string)
USING iceberg;
""")
spark.sql("INSERT INTO TEST_TABLE VALUES (1, 'some data'), (2, 'more data'), (3, 'yet more data')")
spark.sql("SELECT * FROM TEST_TABLE").show()

