from pyiceberg.catalog.rest import RestCatalog
import pyarrow as pa
import os

import requests

### Get Bearer Token ###

POLARIS_HOST = "http://127.0.0.1:8181"

CLIENT_ID = "admin"
CLIENT_SECRET = "password"

token_url = f"{POLARIS_HOST}/v1/oauth/tokens"
payload = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "scope": "PRINCIPAL_ROLE:ALL",
}

response = requests.post(token_url, data=payload)

if response.status_code == 200:
    access_token = response.json().get("access_token")
    print("Token:", access_token)
else:
    print("Error:", response.status_code, response.json())
    exit(1)
print()

### REST Catalog connection ###

catalog = RestCatalog(
    "rest",
    **{
        "uri": "http://127.0.0.1:8181",
        "token": access_token,
        "warehouse": '',
        "s3.endpoint": "http://127.0.0.1:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.path-style-access": "true",
        "s3.ssl.enabled": "false",
    },
)

table = catalog.load_table("default.insert_test")

arrow_table: pa.Table = table.scan().to_arrow()

print(arrow_table)
