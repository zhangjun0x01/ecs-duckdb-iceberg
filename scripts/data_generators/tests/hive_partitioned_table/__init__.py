from scripts.data_generators.tests.base import IcebergTest
import pathlib
import duckdb
import glob


@IcebergTest.register()
class Test(IcebergTest):
    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(path.parent.name)

    def generate(self, con):
        if con.name != 'spark-local':
            raise Error("Not supported for connections that aren't spark-local")
        super().generate(con)

        parquet_files = glob.glob(
            "data/generated/iceberg/spark-local/default/hive_partitioned_table/data/event_date=2024-01-0*/*.parquet"
        )
        for file in parquet_files:
            duckdb.execute(
                f"""
                copy (
                    select
                        *
                    EXCLUDE event_date
                    from '{file}'
                ) to '{file}'
                (
                    FIELD_IDS {{
                        user_id: 2, event_type: 3
                    }}
                );
            """
            )
