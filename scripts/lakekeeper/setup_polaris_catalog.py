CATALOG_URL = "http://localhost:8181/catalog"
MANAGEMENT_URL = "http://localhost:8181/management"
KEYCLOAK_TOKEN_URL = "http://localhost:30080/realms/iceberg/protocol/openid-connect/token"

CLIENT_ID = "spark"
CLIENT_SECRET = "2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52"

# Get an access token from keycloak (the idP server)
response = requests.post(
    url=KEYCLOAK_TOKEN_URL,
    data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "lakekeeper",
    },
    headers={"Content-type": "application/x-www-form-urlencoded"},
)
response.raise_for_status()
access_token = response.json()['access_token']
