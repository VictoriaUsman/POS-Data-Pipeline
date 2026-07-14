from datetime import datetime

# Zones, in pipeline order:
#   bronze   - exact, unmodified vendor payloads (written by the ingestion Lambdas)
#   silver   - validated records (written by the bronze_to_silver Glue job)
#   rejected - records that failed validation, tagged with a reason (same job)
ZONES = ("bronze", "silver", "rejected")


def object_key(zone: str, vendor: str, store_id: str, run_date: datetime, suffix: str = "orders") -> str:
    """Partition scheme shared by every writer (connectors, Glue jobs) so bronze/silver/rejected
    objects for the same run line up under the same year/month/day/vendor/store path."""
    return (
        f"{zone}/pos_vendor={vendor}/store_id={store_id}/"
        f"year={run_date:%Y}/month={run_date:%m}/day={run_date:%d}/"
        f"{suffix}_{run_date:%Y%m%dT%H%M%S}.json"
    )
