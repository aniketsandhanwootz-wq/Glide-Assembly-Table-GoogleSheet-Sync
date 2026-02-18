import os, json, uuid
from typing import Dict, List, Tuple, Set
import requests
from dotenv import load_dotenv
from datetime import datetime
import pytz
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
# --- import path fix (Render cron) ---
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # .../src
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# --- end fix ---
from zai_webhook import emit_zai_event
# ========================
# Load .env
# ========================
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

def need(name: str) -> str:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        raise SystemExit(f"Missing required env: {name} (in {ENV_PATH})")
    return v

def opt(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()

# ========================
# Global config
# ========================
SHEET_ID   = need("GOOGLE_SHEET_ID")
CREDS_JSON = json.loads(need("GOOGLE_CREDENTIALS_JSON"))

LOG_ENABLED      = opt("LOG_ENABLED", "true").lower() in ("1","true","yes","y")
LOG_SHEET_ID     = opt("LOG_SHEET_ID", "") or SHEET_ID
SCRIPT_VERSION   = opt("SCRIPT_VERSION", "")
META_SHEET       = opt("META_SHEET", "_meta")

# ========================
# Job config
# ========================
SHEET_TAB            = need("DASH_SHEET_TAB")
GLIDE_TABLE_NAME     = need("DASH_GLIDE_TABLE_NAME")

SYNCKEY_HEADER       = need("DASH_SHEET_SYNCKEY_HEADER")
GLIDE_SYNCKEY_COL    = need("DASH_GLIDE_SYNCKEY_COL")

MAPPING_JSON_STR     = need("DASH_GLIDE_COLUMNS_JSON")
try:
    # SheetHeader -> GlideColumnId
    MAPPING: Dict[str,str] = json.loads(MAPPING_JSON_STR)
except Exception as e:
    raise SystemExit(f"Invalid DASH_GLIDE_COLUMNS_JSON: {e}")

LOG_DETAILS_TAB      = opt("DASH_LOG_DETAILS_TAB", "dash_change_details")
LOG_SUMMARY_TAB      = opt("DASH_LOG_SUMMARY_TAB", "dash_run_summary")

# ========================
# Debug
# ========================
DEBUG = opt("DASH_DEBUG", "false").lower() in ("1","true","yes","y")

# Sheet may have both "Dashboard Update ID" (synckey) and "SyncKey"
SHEET_KEY_FALLBACK_HEADERS = ["SyncKey"]

# Glide may not always return your synckey col populated; fallback to row id
GLIDE_KEY_FALLBACK_FIELDS = ["$rowID", "rowID"]

def glide_key(g: Dict) -> str:
    v = str(g.get(GLIDE_SYNCKEY_COL, "")).strip()
    if v:
        return v
    for k in GLIDE_KEY_FALLBACK_FIELDS:
        vv = str(g.get(k, "")).strip()
        if vv:
            return vv
    return ""

def sheet_key_from_row(rr: List[str], idx: Dict[str,int]) -> str:
    # primary
    kpos = idx.get(SYNCKEY_HEADER)
    if kpos is not None and kpos < len(rr):
        v = str(rr[kpos]).strip()
        if v:
            return v
    # fallback (if user keeps key in SyncKey column)
    for h in SHEET_KEY_FALLBACK_HEADERS:
        pos = idx.get(h)
        if pos is not None and pos < len(rr):
            v = str(rr[pos]).strip()
            if v:
                return v
    return ""
# ========================
# Glide config (legacy)
# ========================
GLIDE_BASE   = opt("GLIDE_BASE_URL", "https://api.glideapp.io").rstrip("/")
GLIDE_TOKEN  = need("GLIDE_TOKEN")
GLIDE_APP_ID = need("GLIDE_APP_ID")

# ========================
# Time / logging helpers
# ========================
def now_ist() -> str:
    ist = pytz.timezone("Asia/Kolkata")
    return datetime.now(ist).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

DETAIL_COLUMNS = ["timestamp_ist","run_id","action","sync_key","location","column_name","old_value","new_value"]
SUMMARY_COLUMNS = [
    "timestamp_ist","run_id","script_version","job","sheet_tab","glide_table",
    "sheet_rows_before","glide_rows_before","appended_to_sheet","appended_to_glide",
    "result","error_message"
]

# ========================
# Sheets helpers
# ========================
def sheets_service():
    creds = Credentials.from_service_account_info(
        CREDS_JSON, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)

def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def ensure_tab(svc, spreadsheet_id: str, name: str):
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    if any(sh["properties"]["title"] == name for sh in meta.get("sheets", [])):
        return
    svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests":[{"addSheet":{"properties":{"title":name}}}]}
    ).execute()

def get_header(svc, spreadsheet_id: str, tab: str) -> List[str]:
    res = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"'{tab}'!1:1"
    ).execute()
    row = (res.get("values") or [[]])[0]
    return [str(x).strip() for x in row]

def set_header(svc, spreadsheet_id: str, tab: str, header: List[str]):
    end = col_letter(max(1, len(header)))
    svc.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab}'!A1:{end}1",
        valueInputOption="RAW",
        body={"values":[header]}
    ).execute()

def read_all_rows(svc, spreadsheet_id: str, tab: str, cols: int) -> List[List[str]]:
    end = col_letter(max(1, cols))
    res = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab}'!A2:{end}"
    ).execute()
    return res.get("values", [])

def append_rows(svc, spreadsheet_id: str, tab: str, rows: List[List[str]]):
    if not rows:
        return
    svc.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab}'!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()

# ========================
# Logging to sheet
# ========================
def ensure_log_tabs(svc):
    if not LOG_ENABLED:
        return
    ensure_tab(svc, LOG_SHEET_ID, LOG_DETAILS_TAB)
    ensure_tab(svc, LOG_SHEET_ID, LOG_SUMMARY_TAB)

    h1 = get_header(svc, LOG_SHEET_ID, LOG_DETAILS_TAB)
    if h1 != DETAIL_COLUMNS:
        set_header(svc, LOG_SHEET_ID, LOG_DETAILS_TAB, DETAIL_COLUMNS)

    h2 = get_header(svc, LOG_SHEET_ID, LOG_SUMMARY_TAB)
    if h2 != SUMMARY_COLUMNS:
        set_header(svc, LOG_SHEET_ID, LOG_SUMMARY_TAB, SUMMARY_COLUMNS)

def log_details(svc, rows: List[List[str]]):
    if LOG_ENABLED and rows:
        append_rows(svc, LOG_SHEET_ID, LOG_DETAILS_TAB, rows)

def log_summary(svc, row: List[str]):
    if LOG_ENABLED and row:
        append_rows(svc, LOG_SHEET_ID, LOG_SUMMARY_TAB, [row])

# ========================
# Glide helpers (legacy)
# ========================
def glide_query_rows() -> List[Dict]:
    url = f"{GLIDE_BASE}/api/function/queryTables"

    all_rows: List[Dict] = []
    start_at = None

    while True:
        q = {"tableName": GLIDE_TABLE_NAME, "utc": True}
        if start_at:
            q["startAt"] = start_at  # per docs

        body = {"appID": GLIDE_APP_ID, "queries": [q]}

        r = requests.post(
            url,
            headers={"Authorization": f"Bearer {GLIDE_TOKEN}"},
            json=body,
            timeout=120,
        )
        r.raise_for_status()
        j = r.json()

        # ---- Expected shape: list with 1 element per query ----
        # [ { "rows": [...], "next": "..." } ]
        if isinstance(j, list) and j and isinstance(j[0], dict) and "rows" in j[0]:
            block = j[0]
            rows = block.get("rows") or []
            if isinstance(rows, list):
                all_rows.extend(rows)
            start_at = block.get("next")
            if start_at:
                continue
            return all_rows

        # ---- Backward/other shapes (keep your older fallbacks) ----
        if isinstance(j, list):
            # sometimes APIs directly return list of row dicts
            if j and isinstance(j[0], dict) and ("$rowID" in j[0] or "rowID" in j[0]):
                return j
            return []

        if isinstance(j, dict):
            containers = j.get("data") or j.get("result") or j.get("tables") or j.get("Results") or j.get("response")
            if isinstance(containers, list) and containers:
                t0 = containers[0]
                if isinstance(t0, dict) and "rows" in t0:
                    rows = t0.get("rows") or []
                    return rows if isinstance(rows, list) else []
                if isinstance(t0, dict) and ("$rowID" in t0 or "rowID" in t0):
                    return containers
            if isinstance(containers, dict):
                rows = containers.get("rows") or containers.get("data") or containers.get("items") or containers.get("records")
                return rows if isinstance(rows, list) else []

        return []

def glide_add_row(column_values: Dict[str, str]):
    url = f"{GLIDE_BASE}/api/function/mutateTables"
    body = {
        "appID": GLIDE_APP_ID,
        "mutations": [{
            "kind": "add-row-to-table",
            "tableName": GLIDE_TABLE_NAME,
            "columnValues": column_values
        }]
    }
    r = requests.post(url, headers={"Authorization": f"Bearer {GLIDE_TOKEN}"}, json=body, timeout=120)
    r.raise_for_status()
    return r.json()

# ========================
# Main logic
# ========================
def ensure_sheet_headers(svc) -> Tuple[List[str], Dict[str,int]]:
    ensure_tab(svc, SHEET_ID, SHEET_TAB)
    header = get_header(svc, SHEET_ID, SHEET_TAB)
    if not any(x.strip() for x in header):
        header = []
    need_headers = [SYNCKEY_HEADER] + [h for h in MAPPING.keys() if h != SYNCKEY_HEADER]
    missing = [h for h in need_headers if h not in header]
    if missing:
        header = header + missing
        set_header(svc, SHEET_ID, SHEET_TAB, header)
    idx = {h:i for i,h in enumerate(header)}
    return header, idx

def row_pad(row: List[str], w: int) -> List[str]:
    return row[:w] + [""] * max(0, w - len(row))

def get_sheet_synckeys(sheet_rows: List[List[str]], idx: Dict[str,int]) -> Set[str]:
    out = set()
    w = (max(idx.values()) + 1) if idx else 0
    for r in sheet_rows:
        rr = row_pad(r, w)
        k = sheet_key_from_row(rr, idx)
        if k:
            out.add(k)
    return out

def get_glide_synckeys(glide_rows: List[Dict]) -> Set[str]:
    out = set()
    for g in glide_rows:
        k = str(g.get(GLIDE_SYNCKEY_COL, "")).strip()
        if k:
            out.add(k)
    return out

def make_sheet_row_from_glide(g: Dict, header: List[str], idx: Dict[str,int]) -> List[str]:
    row = [""] * len(header)

    k = glide_key(g)
    if k and SYNCKEY_HEADER in idx:
        row[idx[SYNCKEY_HEADER]] = k

    # If sheet also has a "SyncKey" column, populate it too (keeps things consistent)
    if k and "SyncKey" in idx:
        row[idx["SyncKey"]] = k

    for sh, gc in MAPPING.items():
        if sh == SYNCKEY_HEADER:
            continue
        if sh in idx:
            v = g.get(gc, "")
            row[idx[sh]] = "" if v is None else str(v)
    return row

def make_glide_payload_from_sheet(r: List[str], header: List[str], idx: Dict[str,int]) -> Dict[str,str]:
    payload: Dict[str,str] = {}

    key = sheet_key_from_row(r, idx)
    if not key:
        return {}

    # Always set Glide synckey column
    payload[GLIDE_SYNCKEY_COL] = key

    for sh, gc in MAPPING.items():
        if sh == SYNCKEY_HEADER:
            continue
        pos = idx.get(sh)
        if pos is None:
            continue
        payload[gc] = r[pos] if pos < len(r) else ""
    return payload

def run():
    svc = sheets_service()
    ensure_log_tabs(svc)

    run_id = uuid.uuid4().hex[:8]
    ts = now_ist()

    # Ensure sheet headers
    header, idx = ensure_sheet_headers(svc)
    sheet_rows = read_all_rows(svc, SHEET_ID, SHEET_TAB, len(header))
    sheet_rows_before = len(sheet_rows)

    glide_rows = glide_query_rows()
    glide_rows_before = len(glide_rows)
    if DEBUG and glide_rows_before == 0:
        print("WARN: Glide returned 0 rows. Check queryTables parsing / permissions / plan.")

    sheet_keys = get_sheet_synckeys(sheet_rows, idx)
    glide_keys = get_glide_synckeys(glide_rows)

    if DEBUG:
        print("DEBUG sheet_tab:", SHEET_TAB)
        print("DEBUG synckey_header:", SYNCKEY_HEADER, "glide_synckey_col:", GLIDE_SYNCKEY_COL)
        print("DEBUG sheet_rows:", len(sheet_rows), "glide_rows:", len(glide_rows))
        print("DEBUG sheet_keys:", len(sheet_keys), "glide_keys:", len(glide_keys))
        # show a few sample keys
        print("DEBUG sample_sheet_keys:", list(sorted(sheet_keys))[:5])
        print("DEBUG sample_glide_keys:", list(sorted(glide_keys))[:5])
    details: List[List[str]] = []
    dashboard_row_ids_for_trigger: List[str] = []
    # A) Glide -> Sheet (append missing keys)
    to_sheet = [
        g for g in glide_rows
        if str(g.get(GLIDE_SYNCKEY_COL, "")).strip()
        and str(g.get(GLIDE_SYNCKEY_COL, "")).strip() not in sheet_keys
    ]
    sheet_appends = []
    for g in to_sheet:
        k = str(g.get(GLIDE_SYNCKEY_COL, "")).strip()
        # Trigger only for Glide -> Sheet (Glide-originated changes)
        # Prefer the real Glide row id as dashboard_row_id (matches ZAI graph preference).
        row_id = ""
        for rk in ("$rowID", "rowID"):
            v = str(g.get(rk, "")).strip()
            if v:
                row_id = v
                break
        if row_id:
            dashboard_row_ids_for_trigger.append(row_id)
        new_row = make_sheet_row_from_glide(g, header, idx)
        sheet_appends.append(new_row)
        details.append([ts, run_id, "append_sheet", k, "sheet", "(row)", "(blank)", json.dumps(new_row, ensure_ascii=False)])
        sheet_keys.add(k)

    append_rows(svc, SHEET_ID, SHEET_TAB, sheet_appends)
    # Emit DASHBOARD_UPDATED only for the Glide->Sheet rows we appended
    # One event per row_id (best for idempotency and ZAI ingestion).
    for rid in dashboard_row_ids_for_trigger:
        emit_zai_event("DASHBOARD_UPDATED", {"dashboard_row_id": rid})
    # B) Sheet -> Glide (append missing keys)
    to_glide = []
    kpos = idx.get(SYNCKEY_HEADER)
    for r in sheet_rows:
        rr = row_pad(r, len(header))
        k = (rr[kpos] if (kpos is not None) else "").strip()
        if not k:
            continue
        if k not in glide_keys:
            to_glide.append(rr)

    glide_appended = 0
    for rr in to_glide:
        payload = make_glide_payload_from_sheet(rr, header, idx)
        if not payload:
            continue
        k = payload.get(GLIDE_SYNCKEY_COL, "")
        glide_add_row(payload)
        glide_appended += 1
        details.append([ts, run_id, "append_glide", k, "glide", "(row)", "(blank)", json.dumps(payload, ensure_ascii=False)])
        glide_keys.add(k)

    log_details(svc, details)

    log_summary(svc, [
        ts, run_id, SCRIPT_VERSION, "dashboard_append_only", SHEET_TAB, GLIDE_TABLE_NAME,
        str(sheet_rows_before), str(glide_rows_before),
        str(len(sheet_appends)), str(glide_appended),
        "ok", ""
    ])

    print(json.dumps({
        "ok": True,
        "sheet_appended": len(sheet_appends),
        "glide_appended": glide_appended
    }, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        # best-effort summary log
        try:
            svc = sheets_service()
            ensure_log_tabs(svc)
            ts = now_ist()
            run_id = uuid.uuid4().hex[:8]
            log_summary(svc, [
                ts, run_id, SCRIPT_VERSION, "dashboard_append_only", SHEET_TAB, GLIDE_TABLE_NAME,
                "?", "?", "0", "0", "error", str(e)
            ])
        except Exception:
            pass
        raise
