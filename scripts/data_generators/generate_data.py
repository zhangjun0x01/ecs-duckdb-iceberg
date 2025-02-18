from generate_spark_local.generate_iceberg_spark_local import IcebergSparkLocal
from generate_spark_rest.generate_iceberg_spark_rest import IcebergSparkRest

# Example usage:
if __name__ == "__main__":
    spark_local = IcebergSparkRest()
    conn = spark_local.GetConnection()
    spark_local.GenerateTables(conn)
    spark_local.CloseConnection(conn)