# scripts/create_link_token.py
import os

import plaid
from plaid.api import plaid_api
from plaid.model.country_code import CountryCode
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.products import Products


def get_client(env: str, client_id: str, secret: str) -> plaid_api.PlaidApi:
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


if __name__ == "__main__":
    client_id = os.environ["PLAID_CLIENT_ID"]
    prod_secret = os.environ["PLAID_SECRET_PROD"]

    client = get_client("production", client_id, prod_secret)

    req = LinkTokenCreateRequest(
        user=LinkTokenCreateRequestUser(client_user_id="financial-tracker-prod"),
        client_name="Financial Tracker",
        products=[Products("transactions")],
        country_codes=[CountryCode("US")],
        language="en",
    )
    resp = client.link_token_create(req).to_dict()

    print(resp["link_token"])
