PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=iceberg
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# We need this for testing
CORE_EXTENSIONS='httpfs'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Custom makefile targets
data: data_clean
	python3 scripts/test_data_generator/generate_iceberg.py 0.001 lineitem_sf001_v1 1
	python3 scripts/test_data_generator/generate_iceberg.py 0.001 lineitem_sf001_v2 2

data_large: data data_clean
	python3 scripts/test_data_generator/generate_iceberg.py 1 lineitem_sf1_v2 2

data_clean:
	rm -rf data/
