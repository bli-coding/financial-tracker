import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import plaid
from plaid.api import plaid_api
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions

# ✅ Read environment variables (already loaded via direnv)
PLAID_CLIENT_ID = os.environ["PLAID_CLIENT_ID"]
PLAID_SECRET = os.environ["PLAID_SECRET"]
PLAID_ENV = os.environ.get("PLAID_ENV", "sandbox")

# ✅ You will temporarily paste your access_token here (later we store this securely)
ACCESS_TOKEN = os.environ["PLAID_ACCESS_TOKEN"]


def get_client() -> plaid_api.PlaidApi:
    if PLAID_ENV == "sandbox":
        host = plaid.Environment.Sandbox
    elif PLAID_ENV == "development":
        host = plaid.Environment.Development
    else:
        host = plaid.Environment.Production

    configuration = plaid.Configuration(
        host=host,
        api_key={
            "clientId": PLAID_CLIENT_ID,
            "secret": PLAID_SECRET,
        },
    )

    api_client = plaid.ApiClient(configuration)
    return plaid_api.PlaidApi(api_client)


def fetch_tx():
    client = get_client()
    start_date = date.today() - timedelta(days=90)
    end_date = date.today()

    request = TransactionsGetRequest(
        access_token=ACCESS_TOKEN,
        start_date=start_date,
        end_date=end_date,
        options=TransactionsGetRequestOptions(count=500, offset=0),
    )
    response = client.transactions_get(request)
    return response.to_dict()


def json_default(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat()
    # Let json raise the error for anything else unexpected
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


if __name__ == "__main__":

    def json_default(o):
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

    if __name__ == "__main__":
        data = fetch_tx()

        # --- build timestamped path under data/raw/plaidsandbox ---
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = Path("data/raw/plaidsandbox")
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / f"transactions_{timestamp}.json"

        with out_path.open("w") as f:
            json.dump(data, f, indent=2, default=json_default)

        print(f"✅ Saved Plaid sandbox dump to {out_path}")

        # --- extract some basic metadata for logging ---
        total_tx = data.get("total_transactions")
        accounts = data.get("accounts", [])
        account_names = [acct.get("name") or acct.get("official_name") for acct in accounts]

        log_path = Path("docs/experiments.md")
        log_path.parent.mkdir(parents=True, exist_ok=True)

        with log_path.open("a") as log:
            log.write(f"## Plaid sandbox run – {timestamp}\n")
            log.write(f"- JSON path: `{out_path}`\n")
            log.write(f"- Total transactions: {total_tx}\n")
            log.write(f"- Accounts: {', '.join(filter(None, account_names)) or 'N/A'}\n")
            log.write("\n")
