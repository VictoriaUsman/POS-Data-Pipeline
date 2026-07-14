import abc
import json
from datetime import datetime, timezone

import boto3


class BasePOSConnector(abc.ABC):
    vendor: str = None

    def __init__(self, store_id: str, bucket: str, s3_client=None):
        self.store_id = store_id
        self.bucket = bucket
        self.s3 = s3_client or boto3.client("s3")

    @abc.abstractmethod
    def fetch_orders(self, since: datetime) -> list:
        """Poll the vendor API and return raw order records updated since `since`."""

    def s3_key(self, run_date: datetime) -> str:
        return (
            f"pos_vendor={self.vendor}/store_id={self.store_id}/"
            f"year={run_date:%Y}/month={run_date:%m}/day={run_date:%d}/"
            f"orders_{run_date:%Y%m%dT%H%M%S}.json"
        )

    def write_to_s3(self, records: list, run_date: datetime) -> str:
        key = self.s3_key(run_date)
        body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)
        return key

    def run(self, since: datetime):
        records = self.fetch_orders(since)
        if not records:
            return None
        return self.write_to_s3(records, datetime.now(timezone.utc))
