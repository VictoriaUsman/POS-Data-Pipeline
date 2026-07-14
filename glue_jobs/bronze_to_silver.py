"""AWS Glue Python Shell job: bronze -> silver/rejected.

Runs downstream of the ingestion Lambdas (Step Functions triggers this once all of a day's
polling runs are done). Reads that day's bronze/ objects exactly as the vendor sent them,
validates each record, and writes valid/invalid records out to silver/ and rejected/ respectively.
Also watches for vendor schema drift and emits CloudWatch metrics so a schema change surfaces as
an alert instead of silently degrading the pipeline -- see README "Data Validation".

Deliberately a plain boto3 script rather than PySpark -- at ~500-700MB/day across all stores this
is a Python Shell job (per-DPU billing, no cluster startup), not a Spark workload.

Usage: python bronze_to_silver.py --bucket <bucket> [--run-date YYYY-MM-DD]
"""
import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

import boto3

from validation.rules import detect_new_fields, validate_record

VENDOR_RE = re.compile(r"pos_vendor=([^/]+)/")

METRIC_NAMESPACE = "POSPipeline/Validation"
# A required field getting renamed/removed by the vendor rejects effectively every record in a
# run -- alarm on this ratio so that shows up immediately instead of as a slow silent data loss.
REJECTED_RATIO_ALARM_THRESHOLD = 0.10


def _parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--run-date", help="YYYY-MM-DD (UTC), defaults to today")
    args = parser.parse_args(argv)
    run_date = (
        datetime.strptime(args.run_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if args.run_date
        else datetime.now(timezone.utc)
    )
    return args.bucket, run_date


def _list_bronze_keys(s3, bucket: str, run_date: datetime) -> list:
    """Bronze keys don't share a fixed-depth prefix (store_id sits between vendor and the date
    partitions), so list everything under bronze/ and filter by the date suffix -- fine at this
    volume (500-700MB/day across all stores)."""
    date_suffix = f"/year={run_date:%Y}/month={run_date:%m}/day={run_date:%d}/"
    paginator = s3.get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix="bronze/")
        for obj in page.get("Contents", [])
        if date_suffix in obj["Key"]
    ]


def _process_key(s3, bucket: str, key: str) -> dict:
    vendor_match = VENDOR_RE.search(key)
    vendor = vendor_match.group(1) if vendor_match else None

    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    valid, rejected = [], []
    reason_counts = Counter()
    new_fields = set()
    for line in body.splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        new_fields |= detect_new_fields(record, vendor)

        reason = validate_record(record, vendor)
        if reason is None:
            valid.append(record)
        else:
            rejected.append({**record, "_validation_error": reason})
            reason_counts[reason] += 1

    # Swap the zone prefix, keep everything else identical -- reruns land on the same keys.
    silver_key = key.replace("bronze/", "silver/", 1)
    rejected_key = key.replace("bronze/", "rejected/", 1)

    if valid:
        s3.put_object(Bucket=bucket, Key=silver_key, Body=_to_ndjson(valid))
    if rejected:
        s3.put_object(Bucket=bucket, Key=rejected_key, Body=_to_ndjson(rejected))

    return {
        "key": key,
        "vendor": vendor,
        "valid_count": len(valid),
        "rejected_count": len(rejected),
        "reason_counts": reason_counts,
        "new_fields": new_fields,
    }


def _to_ndjson(records: list) -> bytes:
    return "\n".join(json.dumps(r) for r in records).encode("utf-8")


def _report_vendor_summary(cloudwatch, vendor: str, agg: dict) -> None:
    total = agg["valid"] + agg["rejected"]
    ratio = (agg["rejected"] / total) if total else 0.0

    cloudwatch.put_metric_data(
        Namespace=METRIC_NAMESPACE,
        MetricData=[
            {
                "MetricName": "RejectedRatio",
                "Dimensions": [{"Name": "Vendor", "Value": vendor}],
                "Value": ratio,
                "Unit": "Percent",
            },
            {
                "MetricName": "NewFieldCount",
                "Dimensions": [{"Name": "Vendor", "Value": vendor}],
                "Value": len(agg["new_fields"]),
                "Unit": "Count",
            },
        ],
    )

    print(f"[{vendor}] valid={agg['valid']} rejected={agg['rejected']} ratio={ratio:.1%}")
    if agg["reasons"]:
        print(f"[{vendor}] rejection reasons: {dict(agg['reasons'])}")
    if ratio > REJECTED_RATIO_ALARM_THRESHOLD:
        print(
            f"[{vendor}] WARNING: rejected ratio {ratio:.1%} exceeds "
            f"{REJECTED_RATIO_ALARM_THRESHOLD:.0%} -- possible vendor schema change "
            f"(a required field may have been renamed or removed)"
        )
    if agg["new_fields"]:
        print(
            f"[{vendor}] WARNING: new top-level field(s) not in the known schema baseline: "
            f"{sorted(agg['new_fields'])} -- vendor may have added a column; update "
            f"validation/rules.py KNOWN_FIELDS and the README's Normalized Order Model"
        )


def main(argv=None) -> list:
    bucket, run_date = _parse_args(sys.argv[1:] if argv is None else argv)
    s3 = boto3.client("s3")
    cloudwatch = boto3.client("cloudwatch")

    keys = _list_bronze_keys(s3, bucket, run_date)
    results = [_process_key(s3, bucket, key) for key in keys]

    by_vendor = defaultdict(lambda: {"valid": 0, "rejected": 0, "reasons": Counter(), "new_fields": set()})
    for r in results:
        agg = by_vendor[r["vendor"]]
        agg["valid"] += r["valid_count"]
        agg["rejected"] += r["rejected_count"]
        agg["reasons"].update(r["reason_counts"])
        agg["new_fields"] |= r["new_fields"]

    for vendor, agg in by_vendor.items():
        _report_vendor_summary(cloudwatch, vendor, agg)

    total_valid = sum(r["valid_count"] for r in results)
    total_rejected = sum(r["rejected_count"] for r in results)
    print(
        f"bronze_to_silver: processed {len(results)} object(s), "
        f"{total_valid} valid record(s), {total_rejected} rejected record(s)"
    )
    return results


if __name__ == "__main__":
    main()
