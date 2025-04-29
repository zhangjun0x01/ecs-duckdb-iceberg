from tests import IcebergTest
from connections import IcebergConnection
import sys

import argparse


def main():
    parser = argparse.ArgumentParser(description="Generate data for various systems.")
    parser.add_argument(
        "targets",
        nargs="+",
        choices=["polaris", "lakekeeper", "local", "spark-rest"],
        help="Specify one or more targets to generate data for",
    )

    args = parser.parse_args()

    connections = Dict[str, IcebergConnection]
    for target in args.targets:
        connection_class = IcebergConnection.get_class(target)
        connections[target] = connection_class()

    tests = IcebergTest.registry
    for name, test_class in tests:
        test = test_class()
        for con_name in connections:
            con = connections[con_name]
            test.generate(con)


if __name__ == "__main__":
    main()
