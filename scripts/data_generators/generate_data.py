from generate_spark_local.generate_iceberg_spark_local import IcebergSparkLocal
from generate_spark_rest.generate_iceberg_spark_rest import IcebergSparkRest
from generate_polaris_rest.generate_iceberg_polaris_rest import IcebergPolarisRest
import sys

def GenerateSparkRest():
    db2 = IcebergSparkRest()
    conn2 = db2.GetConnection()
    db2.GenerateTables(conn2)
    db2.CloseConnection(conn2)
    del db2
    del conn2

def GenerateSparkLocal():
    db = IcebergSparkLocal()
    conn = db.GetConnection()
    db.GenerateTables(conn)
    db.CloseConnection(conn)
    del db
    del conn


def GeneratePolarisData():
    db = IcebergPolarisRest()
    conn = db.GetConnection()
    db.GenerateTables(conn)
    db.CloseConnection(conn)
    del db
    del conn

if __name__ == "__main__":
    import pdb
    pdb.set_trace()
    argv = sys.argv
    for i in range(1, len(argv)):
        if argv[i] == "polaris":
            GeneratePolarisData()
        elif argv[i] == "local":
            GenerateSparkLocal()
        elif argv[i] == "spark-rest":
            GenerateSparkRest
        else:
            print(f"{argv[i]} not recognized, skipping")
