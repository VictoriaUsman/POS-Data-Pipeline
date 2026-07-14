import base64
import json
import logging
import os

import requests

TEAMS_WEBHOOK_URL = os.environ["TEAMS_WEBHOOK_URL"]

logger = logging.getLogger(__name__)


def notify_teams(event, context):
    """Pub/Sub-triggered Cloud Function. Fires from a Cloud Monitoring alerting
    policy watching BigQuery Data Transfer Service run failures, and posts a
    formatted card into a Microsoft Teams channel via incoming webhook."""
    payload = _parse_event(event)
    card = _build_teams_card(payload)

    try:
        resp = requests.post(TEAMS_WEBHOOK_URL, json=card, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        # The webhook URL itself is the credential (Teams accepts anyone holding it, no
        # further auth) -- requests' default exception message embeds the request URL, so
        # never let that exception (or its traceback) reach logs. `from None` drops the
        # chained original exception from the traceback Cloud Logging would otherwise capture.
        status = getattr(exc.response, "status_code", "unknown")
        logger.error("Failed to post Teams notification (status=%s)", status)
        raise RuntimeError(f"Teams webhook post failed with status {status}") from None


def _parse_event(event: dict) -> dict:
    data = base64.b64decode(event["data"]).decode("utf-8")
    try:
        return json.loads(data)
    except ValueError:
        return {"raw": data}


def _build_teams_card(payload: dict) -> dict:
    incident = payload.get("incident", {})
    summary = incident.get("summary") or "BigQuery Data Transfer Service run failed"
    state = incident.get("state", "unknown")
    resource = incident.get("resource_display_name", "unknown transfer")

    return {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "D32F2F" if state == "open" else "2E7D32",
        "title": "BigQuery Data Transfer Service Alert",
        "text": f"**{summary}**\n\nResource: `{resource}`\nState: `{state}`",
    }
