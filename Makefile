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
	mkdir -p data_generated/data
	python3 scripts/data_generators/generate_data.py

data_large: data data_clean
	python3 scripts/data_generators/generate_data.py

configure_ci: data_clean
	sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
	sudo chmod u+x /usr/local/bin/docker-compose
	python3 -m pip install -r scripts/requirements.txt
	./scripts/start-rest-catalog.sh
	make data

data_clean:
	rm -rf data_generated/
