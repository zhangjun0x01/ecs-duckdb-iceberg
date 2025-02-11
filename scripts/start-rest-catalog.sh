
if test ! -f "./scripts/docker-compose.yml"
then
  # in CI
  echo "Please run from duckdb root."
  exit 1
fi

# cd into scripts where docker-compose file is.
cd scripts

# need to have this happen in the background
set -ex

docker-compose kill
docker-compose rm -f
docker-compose up --detach

pip3 install -r requirements.txt

python3 provision.py

# Would be nice to have rest support in there :)
UNPARTITIONED_TABLE_PATH=$(curl -s http://127.0.0.1:8181/v1/namespaces/default/tables/table_mor_deletes | jq -r '."metadata-location"')

SQL=$(cat <<-END

CREATE SECRET (
  TYPE S3,
  KEY_ID 'admin',
  SECRET 'password',
  ENDPOINT '127.0.0.1:9000',
  URL_STYLE 'path',
  USE_SSL 0
);

SELECT * FROM iceberg_scan('${UNPARTITIONED_TABLE_PATH}');
END

)

if test -f "../build/release/duckdb"
then
  # in CI
  ../build/release/duckdb -s "$SQL"
fi
