
if test ! -f "./scripts/docker-compose.yml"
then
  # in CI
  echo "Please run from duckdb root."
  exit 1
fi

mkdir -p data_generated/data
mkdir -p data_generated/intermediates

# cd into scripts where docker-compose file is.
cd scripts

# need to have this happen in the background
set -ex

docker-compose kill
echo "here 1"
docker-compose rm -f
echo "here 2"
docker-compose up --detach
echo "here 3"
