import re
import os
import json

# Read and parse the JSON file
with open("user_credentials.json", "r") as json_file:
    config = json.load(json_file)

    client_id = config.get("clientId")
    client_secret = config.get("clientSecret")

    if client_id and client_secret:
        # Write client_id and client_secret to separate files
        with open("polaris_client_id.txt", "w") as id_file:
            id_file.write(client_id)

        with open("polaris_client_secret.txt", "w") as secret_file:
            secret_file.write(client_secret)
    else:
        print("clientId or clientSecret not found in config.json")

