# zai_webhook.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

import requests


def _truthy(v: str) -> bool:
    return (v or "").strip().lower() in ("1", "true", "yes", "y", "on")


ZAI_WEBHOOK_URL = (os.getenv("ZAI_WEBHOOK_URL") or "").strip()  # e.g. https://<render-app>.onrender.com/webhooks/sheets
ZAI_WEBHOOK_SECRET = (os.getenv("ZAI_WEBHOOK_SECRET") or os.getenv("WEBHOOK_SECRET") or "").strip()
ZAI_WEBHOOK_TIMEOUT_SECS = float((os.getenv("ZAI_WEBHOOK_TIMEOUT_SECS") or "8").strip() or 8)
ZAI_WEBHOOK_RETRIES = int((os.getenv("ZAI_WEBHOOK_RETRIES") or "2").strip() or 2)

# If true: webhook failures raise and can fail the job (NOT recommended)
ZAI_WEBHOOK_STRICT = _truthy(os.getenv("ZAI_WEBHOOK_STRICT") or "false")


def emit_event(
    *,
    event_type: str,
    payload: Dict[str, Any],
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Best-effort ZAI webhook call. Does NOT affect sync unless ZAI_WEBHOOK_STRICT=true.
    """
    if not ZAI_WEBHOOK_URL:
        return {"ok": False, "skipped": True, "reason": "ZAI_WEBHOOK_URL not set"}

    body = dict(payload or {})
    body["event_type"] = event_type

    m = dict(meta or {})
    # Keep parity with your ZAI graph's expectation: payload.meta is a dict
    body["meta"] = m

    headers = {}
    if ZAI_WEBHOOK_SECRET:
        headers["x-sheets-secret"] = ZAI_WEBHOOK_SECRET

    last_err = None
    for attempt in range(1, ZAI_WEBHOOK_RETRIES + 2):  # retries + first try
        try:
            r = requests.post(
                ZAI_WEBHOOK_URL,
                json=body,
                headers=headers,
                timeout=ZAI_WEBHOOK_TIMEOUT_SECS,
            )
            if 200 <= r.status_code < 300:
                return {"ok": True, "status": r.status_code, "response": _safe_json(r)}
            last_err = RuntimeError(f"webhook status={r.status_code} body={r.text[:500]}")
        except Exception as e:
            last_err = e

        time.sleep(0.4 * attempt)

    if ZAI_WEBHOOK_STRICT:
        raise last_err  # fail hard only if explicitly requested

    return {"ok": False, "error": str(last_err)[:500]}


def _safe_json(r: requests.Response) -> Any:
    try:
        return r.json()
    except Exception:
        return {"text": (r.text or "")[:500]}
