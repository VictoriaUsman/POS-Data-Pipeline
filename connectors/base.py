import abc
import json
import random
import time
from datetime import datetime

import boto3
import requests
from botocore.config import Config

from common.s3_paths import object_key

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 5
BASE_DELAY_SECONDS = 1.0
MAX_DELAY_SECONDS = 60.0

_S3_RETRY_CONFIG = Config(retries={"max_attempts": MAX_ATTEMPTS, "mode": "adaptive"})


class BasePOSConnector(abc.ABC):
    vendor: str = None

    def __init__(self, store_id: str, bucket: str, s3_client=None):
        self.store_id = store_id
        self.bucket = bucket
        self.s3 = s3_client or boto3.client("s3", config=_S3_RETRY_CONFIG)

    @abc.abstractmethod
    def fetch_orders(self, since: datetime, until: datetime) -> list:
        """Poll the vendor API and return raw order records updated in [since, until)."""

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """HTTP call with exponential backoff + jitter, retrying on timeouts/connection
        errors and on 429/5xx responses. Honors a Retry-After header when present."""
        timeout = kwargs.pop("timeout", 30)
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                response = requests.request(method, url, timeout=timeout, **kwargs)
            except requests.exceptions.RequestException:
                if attempt == MAX_ATTEMPTS:
                    raise
                time.sleep(self._backoff_delay(attempt))
                continue

            if response.status_code not in RETRYABLE_STATUS_CODES:
                return response
            if attempt == MAX_ATTEMPTS:
                response.raise_for_status()
            time.sleep(self._retry_after(response) or self._backoff_delay(attempt))

    @staticmethod
    def _backoff_delay(attempt: int) -> float:
        exp = min(MAX_DELAY_SECONDS, BASE_DELAY_SECONDS * (2 ** (attempt - 1)))
        return random.uniform(0, exp)  # full jitter

    @staticmethod
    def _retry_after(response: requests.Response):
        header = response.headers.get("Retry-After")
        if not header:
            return None
        try:
            return float(header)
        except ValueError:
            return None

    def write_to_s3(self, records: list, until: datetime) -> str:
        """Lands the exact vendor payload to the bronze zone, unmodified. Keys on `until` -- the
        run's fixed logical window end -- not wall-clock write time, so a retried run overwrites
        the same bronze key instead of duplicating it. No validation here either; that happens
        downstream in the bronze_to_silver Glue job, so bronze stays replayable even if
        validation rules change later."""
        key = object_key("bronze", self.vendor, self.store_id, until)
        body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)
        return key

    def run(self, since: datetime, until: datetime):
        records = self.fetch_orders(since, until)
        if not records:
            return None
        return self.write_to_s3(records, until)
