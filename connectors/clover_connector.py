from datetime import datetime

from .base import BasePOSConnector


class CloverConnector(BasePOSConnector):
    vendor = "clover"

    def __init__(self, store_id: str, merchant_id: str, api_token: str, bucket: str, **kwargs):
        super().__init__(store_id, bucket, **kwargs)
        self.merchant_id = merchant_id
        self.api_token = api_token
        self.base_url = "https://api.clover.com/v3"

    def fetch_orders(self, since: datetime) -> list:
        headers = {"Authorization": f"Bearer {self.api_token}"}
        limit = 100
        offset = 0
        orders = []
        while True:
            resp = self._request(
                "GET",
                f"{self.base_url}/merchants/{self.merchant_id}/orders",
                headers=headers,
                params={
                    "filter": f"modifiedTime>{int(since.timestamp() * 1000)}",
                    "offset": offset,
                    "limit": limit,
                },
            )
            resp.raise_for_status()
            batch = resp.json().get("elements", [])
            orders.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
        return orders
