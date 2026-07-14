"""One-off/idempotent setup script: expire objects under the `rejected/` zone after a retention
window. `rejected/` records carry full, unmodified vendor payloads (including customer PII on
Shopify orders -- email, phone, billing/shipping address) and, unlike `bronze/`, are meant to be
triaged and acted on, not kept forever -- see README "Runbook: Handling Rejected Records" step 4.

Merges the rule into whatever lifecycle configuration already exists on the bucket (keyed by a
fixed rule ID) rather than overwriting it, since other rules may already be managing bronze/silver
retention independently.

Usage: python configure_rejected_lifecycle.py --bucket <bucket> [--expiration-days 90]
"""
import argparse

import boto3

RULE_ID = "expire-rejected-zone"


def _parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--expiration-days", type=int, default=90)
    return parser.parse_args(argv)


def configure_lifecycle(s3, bucket: str, expiration_days: int) -> None:
    try:
        existing_rules = s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]
    except s3.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchLifecycleConfiguration":
            raise
        existing_rules = []

    other_rules = [r for r in existing_rules if r["ID"] != RULE_ID]
    rejected_rule = {
        "ID": RULE_ID,
        "Status": "Enabled",
        "Filter": {"Prefix": "rejected/"},
        "Expiration": {"Days": expiration_days},
    }

    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={"Rules": other_rules + [rejected_rule]},
    )
    print(f"rejected/ objects in s3://{bucket} will expire after {expiration_days} days")


def main(argv=None) -> None:
    args = _parse_args(argv)
    s3 = boto3.client("s3")
    configure_lifecycle(s3, args.bucket, args.expiration_days)


if __name__ == "__main__":
    main()
