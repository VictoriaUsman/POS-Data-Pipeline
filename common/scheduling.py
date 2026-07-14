"""Derives a fixed, retry-stable polling window from the EventBridge schedule event.

Anchoring on event["time"] -- the schedule's fixed fire time -- instead of datetime.now() at
invocation means every retry of the same triggering event (a Lambda-level retry, or a Step
Functions Task retry) computes the identical [since, until) window, so a retried run re-fetches
the same records instead of a shifted window. Combined with keying the bronze object on `until`
rather than write-time (see connectors/base.py), this is what makes ingestion idempotent under
retries -- see README "Idempotency".
"""
from datetime import datetime, timedelta, timezone


def scheduled_window(event: dict, lookback_minutes: int) -> tuple:
    """Returns (since, until) for this run. `until` is the EventBridge schedule's fixed fire
    time when available (stable across retries); falls back to wall-clock now() for manual/test
    invocations that don't carry an EventBridge `time` field -- those aren't retry-safe."""
    raw_time = (event or {}).get("time")
    until = (
        datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
        if raw_time
        else datetime.now(timezone.utc)
    )
    return until - timedelta(minutes=lookback_minutes), until
