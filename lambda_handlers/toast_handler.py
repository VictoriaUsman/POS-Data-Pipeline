import os
from datetime import datetime, timedelta, timezone

import yaml

from connectors.toast_connector import ToastConnector

BUCKET = os.environ["RAW_BUCKET"]
POLL_LOOKBACK_MINUTES = int(os.environ.get("POLL_LOOKBACK_MINUTES", "15"))
STORES_CONFIG_PATH = os.environ.get("STORES_CONFIG_PATH", "config/stores.yaml")


def _load_stores():
    with open(STORES_CONFIG_PATH) as f:
        return yaml.safe_load(f)["toast"]


def handler(event, context):
    since = datetime.now(timezone.utc) - timedelta(minutes=POLL_LOOKBACK_MINUTES)
    results = []
    for store in _load_stores():
        connector = ToastConnector(
            store_id=store["store_id"],
            restaurant_guid=store["restaurant_guid"],
            client_id=os.environ[store["client_id_env"]],
            client_secret=os.environ[store["client_secret_env"]],
            bucket=BUCKET,
        )
        results.append({"store_id": store["store_id"], **(connector.run(since) or {})})
    return {"processed": results}
