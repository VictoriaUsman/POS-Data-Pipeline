import abc
import json
import random
import time
from datetime import datetime, timezone

import boto3
import requests
from botocore.config import Config

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 5
BASE_DELAY_SECONDS = 1.0
MAX_DELAY_SECONDS = 60.0

_S3_RETRY_CONFIG = Config(retries={"max_attempts": MAX_ATTEMPTS, "mode": "adaptive"})


class BasePOSConnector(abc.ABC):
    vendor: str = None
    required_fields: tuple = ()  # top-level keys that must be present and non-empty on every record

    def __init__(self, store_id: str, bucket: str, s3_client=None):
        self.store_id = store_id
        self.bucket = bucket
        self.s3 = s3_client or boto3.client("s3", config=_S3_RETRY_CONFIG)

    @abc.abstractmethod
    def fetch_orders(self, since: datetime) -> list:
        """Poll the vendor API and return raw order records updated since `since`."""

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

    def s3_key(self, run_date: datetime, zone: str = "raw") -> str:
        return (
            f"{zone}/pos_vendor={self.vendor}/store_id={self.store_id}/"
            f"year={run_date:%Y}/month={run_date:%m}/day={run_date:%d}/"
            f"orders_{run_date:%Y%m%dT%H%M%S}.json"
        )

    def write_to_s3(self, records: list, run_date: datetime, zone: str = "raw") -> str:
        key = self.s3_key(run_date, zone)
        body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)
        return key

    def validate_record(self, record: dict):
        """Return None if the record is valid, else a short reason string.
        Default check is required-field presence; override per vendor for anything stricter."""
        for field in self.required_fields:
            if record.get(field) in (None, ""):
                return f"missing required field: {field}"
        return None

    def run(self, since: datetime):
        records = self.fetch_orders(since)
        if not records:
            return None

        valid, rejected = [], []
        for record in records:
            reason = self.validate_record(record)
            if reason is None:
                valid.append(record)
            else:
                rejected.append({**record, "_validation_error": reason})

        run_date = datetime.now(timezone.utc)
        result = {"raw_key": None, "rejected_key": None, "rejected_count": len(rejected)}
        if valid:
            result["raw_key"] = self.write_to_s3(valid, run_date, zone="raw")
        if rejected:
            result["rejected_key"] = self.write_to_s3(rejected, run_date, zone="rejected")
        return result
