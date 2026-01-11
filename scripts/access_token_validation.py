import os

import plaid
from plaid.api import plaid_api
from plaid.model.country_code import CountryCode
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.products import Products


def plaid_client(env: str, client_id: str, secret: str) -> plaid_api.PlaidApi:
    if env == "sandbox":
        host = plaid.Environment.Sandbox
    elif env == "development":
        host = plaid.Environment.Development
    elif env == "production":
        host = plaid.Environment.Production
    else:
        raise ValueError(f"Unknown env: {env}")

    configuration = plaid.Configuration(
        host=host,
        api_key={"clientId": client_id, "secret": secret},
    )
    api_client = plaid.ApiClient(configuration)
    return plaid_api.PlaidApi(api_client)


# Load from your direnv-exported environment
CLIENT_ID = os.environ["PLAID_CLIENT_ID"]
PROD_SECRET = os.environ["PLAID_SECRET_PROD"]

client = plaid_client("production", CLIENT_ID, PROD_SECRET)

# Create a Link token (credential-only validation; no access_token required)
request = LinkTokenCreateRequest(
    user=LinkTokenCreateRequestUser(client_user_id="financial-tracker-local-test"),
    client_name="Financial Tracker (Local Test)",
    products=[Products("transactions")],
    country_codes=[CountryCode("US")],
    language="en",
)

resp = client.link_token_create(request)
data = resp.to_dict()

# This token is short-lived; treat it as sensitive-ish but it’s not an access_token.
print("✅ Production credentials valid. link_token created.")
print("Expiration:", data.get("expiration"))
print("Request ID:", data.get("request_id"))
