from scripts.data_generators.tests import IcebergTest
from scripts.data_generators.connections import IcebergConnection
import sys
from typing import Dict

import argparse


parser = argparse.ArgumentParser(description="Generate data for various systems.")
parser.add_argument(
    "targets",
    nargs="+",
    choices=["polaris", "lakekeeper", "local", "spark-rest"],
    help="Specify one or more targets to generate data for",
)

args = parser.parse_args()

connections: Dict[str, IcebergConnection] = {}
for target in args.targets:
    connection_class = IcebergConnection.get_class(target)
    connections[target] = connection_class()

tests = IcebergTest.registry
for test_class in tests:
    test = test_class()
    print(f"Generating test '{test.table}'")
    for con_name in connections:
        con = connections[con_name]
        print(f"Generating for '{con_name}'")
        test.generate(con)

if __name__ == "__main__":
    if __package__ is None:
        raise RuntimeError(
            "This script must be run as a module.\n" "Use: python -m scripts.data_generators.generate_data [args]"
        )
