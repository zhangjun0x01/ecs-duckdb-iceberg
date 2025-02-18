from generate_spark_local.generate_iceberg_spark_local import IcebergSparkLocal
from generate_spark_rest.generate_iceberg_spark_rest import IcebergSparkRest

# Example usage:
if __name__ == "__main__":
    for db in [IcebergSparkLocal()]:
        conn = db.GetConnection()
        db.GenerateTables(conn)
        db.CloseConnection(conn)