docker run \
    --name lakekeeper \
    --network="host" \
    -e "LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!" \
    -e "LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@localhost:5433/postgres?sslmode=disable" \
    -e "LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@localhost:5433/postgres?sslmode=disable" \
    -e "LAKEKEEPER__AUTHZ_BACKEND=openfga" \
    -e "LAKEKEEPER__OPENFGA__ENDPOINT=http://localhost:8081" \
    -e "LAKEKEEPER__OPENID_PROVIDER_URI=http://localhost:30080/realms/iceberg" \
    -e "LAKEKEEPER__OPENID_AUDIENCE=lakekeeper" \
    -e "LAKEKEEPER__OPENFGA__CLIENT_ID=openfga" \
    -e "LAKEKEEPER__OPENFGA__CLIENT_SECRET=xqE1vUrifVDKAZdLuz6JAnDxMYLdGu5z" \
    -e "LAKEKEEPER__OPENFGA__TOKEN_ENDPOINT=http://localhost:30080/realms/iceberg/protocol/openid-connect/token" \
    -e "LAKEKEEPER__METRICS_PORT=9002" \
    quay.io/lakekeeper/catalog serve

