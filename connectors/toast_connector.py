import os
from datetime import datetime, timezone

from .base import BasePOSConnector


class ToastConnector(BasePOSConnector):
    vendor = "toast"

    def __init__(
        self,
        store_id: str,
        restaurant_guid: str,
        client_id: str,
        client_secret: str,
        bucket: str,
        **kwargs,
    ):
        super().__init__(store_id, bucket, **kwargs)
        self.restaurant_guid = restaurant_guid
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = os.environ.get("TOAST_API_BASE_URL", "https://ws-api.toasttab.com")
        self._token = None

    def _authenticate(self) -> str:
        if self._token:
            return self._token
        resp = self._request(
            "POST",
            f"{self.base_url}/authentication/v1/authentication/login",
            json={
                "clientId": self.client_id,
                "clientSecret": self.client_secret,
                "userAccessType": "TOAST_MACHINE_CLIENT",
            },
        )
        resp.raise_for_status()
        self._token = resp.json()["token"]["accessToken"]
        return self._token

    def fetch_orders(self, since: datetime) -> list:
        headers = {
            "Authorization": f"Bearer {self._authenticate()}",
            "Toast-Restaurant-External-ID": self.restaurant_guid,
        }
        # Toast recommends ordersBulk with startDate/endDate over filtering by businessDate --
        # businessDate only reflects an order's creation day and misses same-day-created orders
        # that were modified later.
        resp = self._request(
            "GET",
            f"{self.base_url}/orders/v2/ordersBulk",
            headers=headers,
            params={
                "startDate": since.isoformat(),
                "endDate": datetime.now(timezone.utc).isoformat(),
            },
        )
        resp.raise_for_status()
        return resp.json()
