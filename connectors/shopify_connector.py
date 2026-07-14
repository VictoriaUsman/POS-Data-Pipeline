import os
from datetime import datetime

from .base import BasePOSConnector


class ShopifyConnector(BasePOSConnector):
    vendor = "shopify"
    required_fields = ("id", "created_at", "total_price")  # verify against Shopify Orders API docs

    def __init__(self, store_id: str, shop_domain: str, access_token: str, bucket: str, **kwargs):
        super().__init__(store_id, bucket, **kwargs)
        self.shop_domain = shop_domain
        self.access_token = access_token
        self.api_version = os.environ.get("SHOPIFY_API_VERSION", "2024-10")

    def fetch_orders(self, since: datetime) -> list:
        url = f"https://{self.shop_domain}/admin/api/{self.api_version}/orders.json"
        headers = {"X-Shopify-Access-Token": self.access_token}
        params = {"status": "any", "updated_at_min": since.isoformat()}

        orders = []
        while url:
            resp = self._request("GET", url, headers=headers, params=params)
            resp.raise_for_status()
            orders.extend(resp.json().get("orders", []))
            url = self._next_page_url(resp.headers.get("Link"))
            params = None  # next-page URL already carries the query string
        return orders

    @staticmethod
    def _next_page_url(link_header):
        if not link_header:
            return None
        for part in link_header.split(","):
            if 'rel="next"' in part:
                return part.split(";")[0].strip().strip("<>")
        return None
