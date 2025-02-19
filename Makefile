PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=iceberg
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# We need this for testing
CORE_EXTENSIONS='httpfs'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

start-rest-catalog: install_deps
	./scripts/start-rest-catalog.sh

install_deps:
	python3 -m pip install -r scripts/requirements.txt

# Custom makefile targets
data: install_deps data_clean
	mkdir -p data
	python3 scripts/data_generators/generate_data.py

data_large: data data_clean
	python3 scripts/data_generators/generate_data.py

data_clean:
	rm -rf data/generated
