# zai_webhook.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

import requests


def _truthy(v: str) -> bool:
    return (v or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _opt(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def emit_zai_event(event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fire-and-forget style webhook emitter to ZAI.

    Uses the SAME endpoint style as your AppSheet/AppsScript triggers:
      POST <ZAI_WEBHOOK_URL>   (recommended: https://<host>/webhooks/sheets)
      Header: x-sheets-secret: <ZAI_WEBHOOK_SECRET>

    Env:
      ZAI_WEBHOOK_URL        -> required to emit
      ZAI_WEBHOOK_SECRET     -> required to emit
      ZAI_WEBHOOK_ENABLED    -> default true
      ZAI_WEBHOOK_TIMEOUT_S  -> default 20
      ZAI_WEBHOOK_RETRIES    -> default 2 (total attempts = 1 + retries)
      ZAI_WEBHOOK_SOURCE     -> default "sync_script"

    Returns a small dict with ok + status for logs.
    Never raises (won't break sync jobs).
    """
    enabled = _truthy(_opt("ZAI_WEBHOOK_ENABLED", "true"))
    url = _opt("ZAI_WEBHOOK_URL", "")
    secret = _opt("ZAI_WEBHOOK_SECRET", "")

    if not enabled or not url or not secret:
        return {"ok": False, "skipped": True, "reason": "webhook disabled or missing env"}

    timeout_s = float(_opt("ZAI_WEBHOOK_TIMEOUT_S", "20") or 20)
    retries = int(_opt("ZAI_WEBHOOK_RETRIES", "2") or 2)

    body = dict(payload or {})
    body["event_type"] = event_type

    meta = body.get("meta") if isinstance(body.get("meta"), dict) else {}
    meta.setdefault("source", _opt("ZAI_WEBHOOK_SOURCE", "sync_script"))
    body["meta"] = meta

    headers = {
        "x-sheets-secret": secret,
        "content-type": "application/json",
    }

    last_err: Optional[str] = None
    for attempt in range(0, retries + 1):
        try:
            r = requests.post(url, json=body, headers=headers, timeout=timeout_s)
            if 200 <= r.status_code < 300:
                return {"ok": True, "status": r.status_code}
            last_err = f"HTTP {r.status_code}: {r.text[:500]}"
        except Exception as e:
            last_err = str(e)[:500]

        # backoff
        time.sleep(0.6 * (attempt + 1))

    return {"ok": False, "skipped": False, "error": last_err or "unknown error"}
