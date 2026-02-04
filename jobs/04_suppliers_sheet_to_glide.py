#!/usr/bin/env python3
"""
suppliers_sheet_to_glide.py

Idempotent Sheet -> Glide sync for Suppliers.

Key points:
- Uses SHEET "ID" as the unique business key (maps to Glide column O0rtV in your env).
- NO dependency on storing Glide RowID in the sheet.
- Fixes re-ingest on cron re-runs by fetching ALL Glide rows via pagination (startAt/next).
- Optional safety: avoids overwriting non-empty Glide values with empty Sheet values.
- Robust Sheets logging with auto sheet resize to avoid "exceeds grid limits".

ENV expected (as you shared):
  SUP_SHEET_TAB
  SUP_GLIDE_TABLE_NAME
  SUP_SHEET_KEY_HEADER
  SUP_SHEET_GLIDE_ROWID_HEADER
  SUP_GLIDE_COLUMNS_JSON
  GLIDE_TOKEN, GLIDE_APP_ID, (optional GLIDE_BASE_URL)
  GOOGLE_SHEET_ID, GOOGLE_CREDENTIALS_JSON
"""

import os, json, uuid
from typing import Dict, List, Tuple
import requests
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import pytz

import time, random, ssl, socket
import httplib2
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_httplib2 import AuthorizedHttp


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

def opt(name: str, default: str="") -> str:
    return os.getenv(name, default).strip()

# ========================
# Global config
# ========================
SHEET_ID     = need("GOOGLE_SHEET_ID")
CREDS_JSON   = json.loads(need("GOOGLE_CREDENTIALS_JSON"))

LOG_ENABLED      = opt("LOG_ENABLED","true").lower() in ("1","true","yes","y")
LOG_SHEET_ID     = opt("LOG_SHEET_ID","") or SHEET_ID
SCRIPT_VERSION   = opt("SCRIPT_VERSION","")

# ========================
# Job config
# ========================
SHEET_TAB = need("SUP_SHEET_TAB")
GLIDE_TABLE_NAME = need("SUP_GLIDE_TABLE_NAME")

SHEET_KEY_HEADER = need("SUP_SHEET_KEY_HEADER")                 # ID
SHEET_ROWID_HEADER = need("SUP_SHEET_GLIDE_ROWID_HEADER")       # 🔒 Supplier ID (sheet column)

MAPPING_JSON_STR = need("SUP_GLIDE_COLUMNS_JSON")
try:
    # SheetHeader -> GlideColumnId
    MAPPING: Dict[str,str] = json.loads(MAPPING_JSON_STR)
except Exception as e:
    raise SystemExit(f"Invalid SUP_GLIDE_COLUMNS_JSON: {e}")

# Must be empty/disabled (you mapped o0rtT via MAPPING now)
GLIDE_POINTER_COL = opt("SUP_GLIDE_POINTER_COL","")

LOG_DETAILS_TAB = opt("SUP_LOG_DETAILS_TAB","sup_change_details")
LOG_SUMMARY_TAB = opt("SUP_LOG_SUMMARY_TAB","sup_run_summary")

# ========================
# Glide
# ========================
GLIDE_BASE   = opt("GLIDE_BASE_URL", "https://api.glideapp.io").rstrip("/")
GLIDE_TOKEN  = need("GLIDE_TOKEN")
GLIDE_APP_ID = need("GLIDE_APP_ID")

SESSION = requests.Session()
SESSION.headers.update({"Authorization": f"Bearer {GLIDE_TOKEN}"})


# ========================
# Time / logging
# ========================
def now_ist() -> str:
    ist = pytz.timezone("Asia/Kolkata")
    return datetime.now(ist).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

DETAIL_COLUMNS = ["timestamp_ist","run_id","action","supplier_id","location","column_name","old_value","new_value"]
SUMMARY_COLUMNS = [
    "timestamp_ist","run_id","script_version","job","sheet_tab","glide_table",
    "sheet_rows","glide_rows","glide_added","glide_updated","sheet_pointer_updates",
    "result","error_message"
]

def _clip_cell(s: str, limit: int = 45000) -> str:
    s = "" if s is None else str(s)
    if len(s) <= limit:
        return s
    return s[:limit] + f"...(clipped {len(s)-limit} chars)"


# ========================
# Sheets helpers (robust)
# ========================
def sheets_service():
    creds = Credentials.from_service_account_info(
        CREDS_JSON, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    http = AuthorizedHttp(creds, http=httplib2.Http(timeout=120))
    return build("sheets", "v4", http=http, cache_discovery=False)

def api_execute(req, what: str = "", max_attempts: int = 6):
    base = 0.8
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            return req.execute(num_retries=3)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError, socket.timeout, TimeoutError) as e:
            last_err = e
        except httplib2.HttpLib2Error as e:
            last_err = e
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            if status in (429, 500, 502, 503, 504):
                last_err = e
            else:
                raise
        sleep_s = min(20.0, base * (2 ** (attempt - 1)) + random.random())
        time.sleep(sleep_s)
    raise last_err  # type: ignore

def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

# Grid size cache
_SHEET_META_CACHE: Dict[str, Dict[str, Dict[str, int]]] = {}  # spreadsheet_id -> title -> props
_NEXT_ROW_CACHE: Dict[Tuple[str, str], int] = {}              # (spreadsheet_id, tab) -> next_row

def _load_sheet_meta(svc, spreadsheet_id: str) -> Dict[str, Dict[str, int]]:
    meta = api_execute(svc.spreadsheets().get(spreadsheetId=spreadsheet_id), what="spreadsheets.get")
    out: Dict[str, Dict[str, int]] = {}
    for sh in meta.get("sheets", []):
        p = sh.get("properties", {})
        title = p.get("title", "")
        grid = p.get("gridProperties", {}) or {}
        out[title] = {
            "sheetId": int(p.get("sheetId")),
            "rowCount": int(grid.get("rowCount", 1000)),
            "columnCount": int(grid.get("columnCount", 26)),
        }
    _SHEET_META_CACHE[spreadsheet_id] = out
    return out

def _get_tab_props(svc, spreadsheet_id: str, tab: str) -> Dict[str, int]:
    tabs = _SHEET_META_CACHE.get(spreadsheet_id)
    if tabs is None or tab not in tabs:
        tabs = _load_sheet_meta(svc, spreadsheet_id)
    return tabs.get(tab) or {}

def ensure_tab(svc, spreadsheet_id: str, name: str):
    tabs = _SHEET_META_CACHE.get(spreadsheet_id)
    if tabs is None:
        tabs = _load_sheet_meta(svc, spreadsheet_id)

    if name in tabs:
        return

    api_execute(
        svc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests":[{"addSheet":{"properties":{"title":name}}}]}
        ),
        what="spreadsheets.batchUpdate(addSheet)"
    )
    _load_sheet_meta(svc, spreadsheet_id)

def ensure_grid_size(svc, spreadsheet_id: str, tab: str, min_rows: int, min_cols: int, row_buffer: int = 500):
    props = _get_tab_props(svc, spreadsheet_id, tab)
    if not props:
        ensure_tab(svc, spreadsheet_id, tab)
        props = _get_tab_props(svc, spreadsheet_id, tab)

    sheet_id = props["sheetId"]
    row_count = props["rowCount"]
    col_count = props["columnCount"]

    need_rows = min_rows > row_count
    need_cols = min_cols > col_count
    if not (need_rows or need_cols):
        return

    new_rows = max(row_count, min_rows + row_buffer) if need_rows else row_count
    new_cols = max(col_count, min_cols) if need_cols else col_count

    api_execute(
        svc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [{
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {
                                "rowCount": new_rows,
                                "columnCount": new_cols,
                            },
                        },
                        "fields": "gridProperties.rowCount,gridProperties.columnCount",
                    }
                }]
            }
        ),
        what=f"spreadsheets.batchUpdate(resize:{tab})"
    )

    tabs = _SHEET_META_CACHE.get(spreadsheet_id)
    if tabs is None:
        tabs = _load_sheet_meta(svc, spreadsheet_id)
    if tab in tabs:
        tabs[tab]["rowCount"] = new_rows
        tabs[tab]["columnCount"] = new_cols

def get_header(svc, spreadsheet_id: str, tab: str) -> List[str]:
    res = api_execute(
        svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"'{tab}'!1:1"),
        what="values.get(header)"
    )
    return [str(x).strip() for x in (res.get("values") or [[]])[0]]

def set_header(svc, spreadsheet_id: str, tab: str, header: List[str]):
    end = col_letter(max(1, len(header)))
    ensure_grid_size(svc, spreadsheet_id, tab, min_rows=1, min_cols=len(header))
    api_execute(
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{tab}'!A1:{end}1",
            valueInputOption="RAW",
            body={"values":[header]}
        ),
        what="values.update(header)"
    )

def read_all_rows(svc, spreadsheet_id: str, tab: str, cols: int) -> List[List[str]]:
    end = col_letter(max(1, cols))
    res = api_execute(
        svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"'{tab}'!A2:{end}"),
        what="values.get(rows)"
    )
    return res.get("values", [])

def pad_row(row: List[str], w: int) -> List[str]:
    return row[:w] + [""] * max(0, w - len(row))

def _sheet_next_row(svc, spreadsheet_id: str, tab: str) -> int:
    k = (spreadsheet_id, tab)
    if k in _NEXT_ROW_CACHE:
        return _NEXT_ROW_CACHE[k]
    res = api_execute(
        svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"'{tab}'!A:A"),
        what=f"values.get({tab}!A:A)"
    )
    vals = res.get("values", [])
    nxt = len(vals) + 1
    _NEXT_ROW_CACHE[k] = nxt
    return nxt

def append_rows_fast(svc, spreadsheet_id: str, tab: str, rows: List[List[str]], chunk_rows: int = 1000):
    if not rows:
        return

    start_row = _sheet_next_row(svc, spreadsheet_id, tab)

    for i in range(0, len(rows), chunk_rows):
        chunk = rows[i:i+chunk_rows]
        width = max(1, max(len(r) for r in chunk))
        end_col = col_letter(width)

        r0 = start_row + i
        r1 = r0 + len(chunk) - 1

        ensure_grid_size(svc, spreadsheet_id, tab, min_rows=r1, min_cols=width, row_buffer=1000)

        chunk_norm = [pad_row([_clip_cell(x) for x in row], width) for row in chunk]

        api_execute(
            svc.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "valueInputOption": "RAW",
                    "data": [{
                        "range": f"'{tab}'!A{r0}:{end_col}{r1}",
                        "values": chunk_norm
                    }]
                }
            ),
            what=f"values.batchUpdate(append_like:{tab})"
        )

    _NEXT_ROW_CACHE[(spreadsheet_id, tab)] = start_row + len(rows)


# ========================
# Logging tabs
# ========================
def ensure_log_tabs(svc):
    if not LOG_ENABLED:
        return
    ensure_tab(svc, LOG_SHEET_ID, LOG_DETAILS_TAB)
    ensure_tab(svc, LOG_SHEET_ID, LOG_SUMMARY_TAB)
    if get_header(svc, LOG_SHEET_ID, LOG_DETAILS_TAB) != DETAIL_COLUMNS:
        set_header(svc, LOG_SHEET_ID, LOG_DETAILS_TAB, DETAIL_COLUMNS)
    if get_header(svc, LOG_SHEET_ID, LOG_SUMMARY_TAB) != SUMMARY_COLUMNS:
        set_header(svc, LOG_SHEET_ID, LOG_SUMMARY_TAB, SUMMARY_COLUMNS)

def log_details(svc, rows: List[List[str]]):
    if LOG_ENABLED and rows:
        append_rows_fast(svc, LOG_SHEET_ID, LOG_DETAILS_TAB, rows, chunk_rows=1000)

def log_summary(svc, row: List[str]):
    if LOG_ENABLED and row:
        append_rows_fast(svc, LOG_SHEET_ID, LOG_SUMMARY_TAB, [row], chunk_rows=1000)


# ========================
# Glide helpers (PAGINATED)
# ========================
def glide_query_rows() -> List[Dict]:
    """
    Fetch ALL rows from Glide table using pagination.
    This prevents cron re-runs from 'missing' rows and re-adding them.
    """
    url = f"{GLIDE_BASE}/api/function/queryTables"

    all_rows: List[Dict] = []
    start_at = None

    while True:
        q = {"tableName": GLIDE_TABLE_NAME, "utc": True}
        if start_at:
            q["startAt"] = start_at

        body = {"appID": GLIDE_APP_ID, "queries": [q]}
        r = SESSION.post(url, json=body, timeout=180)
        r.raise_for_status()
        j = r.json()

        # expected: [ { "rows": [...], "next": "..." } ]
        if isinstance(j, list) and j and isinstance(j[0], dict) and "rows" in j[0]:
            block = j[0]
            rows = block.get("rows") or []
            if isinstance(rows, list):
                all_rows.extend(rows)

            start_at = block.get("next")
            if start_at:
                continue
            return all_rows

        # fallback (unexpected response)
        return all_rows

def glide_mutate(mutations: List[Dict]) -> Dict:
    url = f"{GLIDE_BASE}/api/function/mutateTables"
    body = {"appID": GLIDE_APP_ID, "mutations": mutations}
    r = SESSION.post(url, json=body, timeout=180)
    r.raise_for_status()
    return r.json()

def glide_rowid(g: Dict) -> str:
    return str(g.get("$rowID") or g.get("rowID") or "").strip()

def norm_key(s: str) -> str:
    return ("" if s is None else str(s)).strip().upper()


# ========================
# Main
# ========================
def run():
    if GLIDE_POINTER_COL.strip():
        raise SystemExit(
            "SUP_GLIDE_POINTER_COL must be empty. "
            "Use SUP_GLIDE_COLUMNS_JSON to map 🔒 Supplier ID -> o0rtT."
        )

    svc = sheets_service()
    ensure_log_tabs(svc)

    run_id = uuid.uuid4().hex[:8]
    ts = now_ist()

    ensure_tab(svc, SHEET_ID, SHEET_TAB)
    header = get_header(svc, SHEET_ID, SHEET_TAB)
    header = [h for h in header if h is not None and str(h).strip() != ""]

    required_headers = [SHEET_KEY_HEADER, SHEET_ROWID_HEADER] + list(MAPPING.keys())
    missing = [h for h in required_headers if h not in header]
    if missing:
        header = header + missing
        set_header(svc, SHEET_ID, SHEET_TAB, header)

    idx = {h:i for i,h in enumerate(header)}
    rows = read_all_rows(svc, SHEET_ID, SHEET_TAB, len(header))
    rows_padded = [pad_row(r, len(header)) for r in rows]

    key_pos = idx[SHEET_KEY_HEADER]

    # business key: sheet ID -> glide Supplier ID column
    supplier_id_glide_col = MAPPING.get(SHEET_KEY_HEADER)
    if not supplier_id_glide_col:
        raise SystemExit("SUP_GLIDE_COLUMNS_JSON must include mapping for sheet key header (ID)")

    glide_rows = glide_query_rows()

    # build: SupplierID -> Glide row dict
    glide_by_supplier: Dict[str, Dict] = {}
    for g in glide_rows:
        sid = norm_key(g.get(supplier_id_glide_col, ""))
        if sid and sid not in glide_by_supplier:
            glide_by_supplier[sid] = g

    details: List[List[str]] = []
    glide_added = 0
    glide_updated = 0

    mutations: List[Dict] = []
    MUTATION_CHUNK = int(opt("GLIDE_MUTATION_CHUNK", "200") or "200")

    def flush_mutations():
        nonlocal mutations
        if not mutations:
            return
        glide_mutate(mutations)
        mutations = []

    sheet_pointer_updates = 0  # by design (no RowID writeback)

    # OPTIONAL safety: do not overwrite non-empty Glide with empty Sheet
    SKIP_EMPTY_OVERWRITE = opt("SUP_SKIP_EMPTY_OVERWRITE", "true").lower() in ("1","true","yes","y")

    for _, r in enumerate(rows_padded, start=2):
        sid = norm_key(r[key_pos] or "")
        if not sid:
            continue

        g = glide_by_supplier.get(sid)

        # ADD missing in Glide
        if not g:
            payload: Dict[str, str] = {}
            for sh, gc in MAPPING.items():
                pos = idx.get(sh)
                if pos is None:
                    continue
                payload[gc] = (r[pos] if pos < len(r) else "")

            mutations.append({
                "kind": "add-row-to-table",
                "tableName": GLIDE_TABLE_NAME,
                "columnValues": payload
            })
            glide_added += 1

            details.append([
                ts, run_id, "add_glide", sid, "glide",
                "(row)", "(blank)", _clip_cell(json.dumps(payload, ensure_ascii=False))
            ])

            if len(mutations) >= MUTATION_CHUNK:
                flush_mutations()
            continue

        # UPDATE existing (only diffs; do not rewrite key)
        row_id = glide_rowid(g)
        payload: Dict[str, str] = {}

        for sh, gc in MAPPING.items():
            if sh == SHEET_KEY_HEADER:
                continue  # do not rewrite key

            pos = idx.get(sh)
            if pos is None:
                continue

            sv = (r[pos] if pos < len(r) else "")
            gv = "" if g.get(gc) is None else str(g.get(gc))

            if SKIP_EMPTY_OVERWRITE and str(sv).strip() == "" and str(gv).strip() != "":
                continue

            if str(sv) != str(gv):
                payload[gc] = sv
                details.append([ts, run_id, "update_glide", sid, "glide", sh, _clip_cell(gv), _clip_cell(sv)])

        if payload and row_id:
            mutations.append({
                "kind": "set-columns-in-row",
                "tableName": GLIDE_TABLE_NAME,
                "rowID": row_id,
                "columnValues": payload
            })
            glide_updated += 1

            if len(mutations) >= MUTATION_CHUNK:
                flush_mutations()

    flush_mutations()

    # logs (auto-resize prevents grid limit crash)
    log_details(svc, details)
    log_summary(svc, [
        ts, run_id, SCRIPT_VERSION, "suppliers_sheet_to_glide", SHEET_TAB, GLIDE_TABLE_NAME,
        str(len(rows_padded)), str(len(glide_rows)),
        str(glide_added), str(glide_updated), str(sheet_pointer_updates),
        "ok", ""
    ])

    print(json.dumps({
        "ok": True,
        "glide_added": glide_added,
        "glide_updated": glide_updated,
        "sheet_pointer_updates": sheet_pointer_updates,
        "glide_rows_seen": len(glide_rows),
        "sheet_rows_seen": len(rows_padded),
    }, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        try:
            svc = sheets_service()
            ensure_log_tabs(svc)
            ts = now_ist()
            run_id = uuid.uuid4().hex[:8]
            log_summary(svc, [
                ts, run_id, SCRIPT_VERSION, "suppliers_sheet_to_glide", SHEET_TAB, GLIDE_TABLE_NAME,
                "?", "?", "0", "0", "0", "error", _clip_cell(str(e))
            ])
        except Exception:
            pass
        raise