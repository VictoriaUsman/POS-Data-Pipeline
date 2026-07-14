import os
from datetime import datetime, timedelta, timezone

import yaml

from connectors.clover_connector import CloverConnector

BUCKET = os.environ["RAW_BUCKET"]
POLL_LOOKBACK_MINUTES = int(os.environ.get("POLL_LOOKBACK_MINUTES", "15"))
STORES_CONFIG_PATH = os.environ.get("STORES_CONFIG_PATH", "config/stores.yaml")


def _load_stores():
    with open(STORES_CONFIG_PATH) as f:
        return yaml.safe_load(f)["clover"]


def handler(event, context):
    since = datetime.now(timezone.utc) - timedelta(minutes=POLL_LOOKBACK_MINUTES)
    results = []
    for store in _load_stores():
        connector = CloverConnector(
            store_id=store["store_id"],
            merchant_id=store["merchant_id"],
            api_token=os.environ[store["api_token_env"]],
            bucket=BUCKET,
        )
        results.append({"store_id": store["store_id"], "s3_key": connector.run(since)})
    return {"processed": results}
