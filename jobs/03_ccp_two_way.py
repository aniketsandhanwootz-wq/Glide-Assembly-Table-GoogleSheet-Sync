import os, json, uuid
from typing import Dict, List, Tuple, Any, Optional
import requests
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import time
import pytz
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

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
META_SHEET        = opt("META_SHEET","_meta")

# ========================
# Log safety limits (avoid BrokenPipe on huge appends)
# ========================
LOG_VALUE_MAX_CHARS = int(opt("LOG_VALUE_MAX_CHARS", "2000") or 2000)   # per-cell max chars in log
LOG_DETAIL_BATCH    = int(opt("LOG_DETAIL_BATCH", "200") or 200)        # rows per append request
LOG_DETAIL_MAX_ROWS = int(opt("LOG_DETAIL_MAX_ROWS", "10000") or 10000) # cap per run (safety)
# ========================
# Job config (CCP)
# ========================
SHEET_TAB = need("CCP_SHEET_TAB")
GLIDE_TABLE_NAME = need("CCP_GLIDE_TABLE_NAME")

SHEET_KEY_HEADER = need("CCP_SHEET_KEY_HEADER")                 # "CCP ID"
SHEET_ROWID_HEADER = need("CCP_SHEET_GLIDE_ROWID_HEADER")       # "🔒 Row ID"

SHEET_UPDATED_AT_HEADER = need("CCP_SHEET_UPDATED_AT_HEADER")
SHEET_UPDATED_BY_HEADER = need("CCP_SHEET_UPDATED_BY_HEADER")
GLIDE_UPDATED_AT_COL = need("CCP_GLIDE_UPDATED_AT_COL")
GLIDE_UPDATED_BY_COL = need("CCP_GLIDE_UPDATED_BY_COL")

CONFLICT_WINNER = opt("CCP_CONFLICT_WINNER","sheet").lower()    # sheet | glide

MAPPING_JSON_STR = need("CCP_GLIDE_COLUMNS_JSON")
try:
    # SheetHeader -> GlideColumnId
    MAPPING: Dict[str,str] = json.loads(MAPPING_JSON_STR)
except Exception as e:
    raise SystemExit(f"Invalid CCP_GLIDE_COLUMNS_JSON: {e}")

LOG_DETAILS_TAB = opt("CCP_LOG_DETAILS_TAB","ccp_change_details")
LOG_SUMMARY_TAB = opt("CCP_LOG_SUMMARY_TAB","ccp_run_summary")

# ========================
# Glide (legacy)
# ========================
GLIDE_BASE   = opt("GLIDE_BASE_URL", "https://api.glideapp.io").rstrip("/")
GLIDE_TOKEN  = need("GLIDE_TOKEN")
GLIDE_APP_ID = need("GLIDE_APP_ID")

# ========================
# Time / logging
# ========================
def now_ist() -> str:
    ist = pytz.timezone("Asia/Kolkata")
    return datetime.now(ist).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

DETAIL_COLUMNS = ["timestamp_ist","run_id","action","ccp_id","location","column_name","old_value","new_value"]
SUMMARY_COLUMNS = [
    "timestamp_ist","run_id","script_version","job","sheet_tab","glide_table",
    "sheet_rows","glide_rows","sheet_appended","sheet_updated_cells",
    "glide_added","glide_updated","result","error_message"
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
    res = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"'{tab}'!1:1").execute()
    return [str(x).strip() for x in (res.get("values") or [[]])[0]]

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
    res = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"'{tab}'!A2:{end}").execute()
    return res.get("values", [])

def append_rows(svc, spreadsheet_id: str, tab: str, rows: List[List[str]]):
    if not rows:
        return

    # Retry on transient network/connection issues (BrokenPipe, etc.)
    last_err = None
    for attempt in range(1, 4):
        try:
            svc.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"'{tab}'!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": rows}
            ).execute()
            return
        except BrokenPipeError as e:
            last_err = e
            time.sleep(1.0 * attempt)
        except Exception as e:
            # Some BrokenPipe cases surface as generic exceptions from httplib2
            msg = str(e).lower()
            if "broken pipe" in msg or "connection" in msg:
                last_err = e
                time.sleep(1.0 * attempt)
                continue
            raise

    raise last_err if last_err else RuntimeError("append_rows failed after retries")

def batch_update_cells(svc, spreadsheet_id: str, tab: str, updates: List[Tuple[int,int,str]]):
    if not updates:
        return
    data = []
    for r, c, v in updates:
        colL = col_letter(c)
        data.append({"range": f"'{tab}'!{colL}{r}:{colL}{r}", "values": [[v]]})
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"valueInputOption":"RAW","data":data}
    ).execute()

def pad_row(row: List[str], w: int) -> List[str]:
    return row[:w] + [""] * max(0, w - len(row))

def _clip(v: Any, n: int = LOG_VALUE_MAX_CHARS) -> str:
    if v is None:
        return ""
    s = str(v)
    if len(s) <= n:
        return s
    return s[: max(0, n - 12)] + f"...({len(s)}c)"

def _chunked(lst: List[Any], size: int) -> List[List[Any]]:
    return [lst[i:i+size] for i in range(0, len(lst), size)]
# ========================
# Logging to sheet
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
    if not (LOG_ENABLED and rows):
        return

    # Hard cap to avoid runaway log volume
    if len(rows) > LOG_DETAIL_MAX_ROWS:
        rows = rows[:LOG_DETAIL_MAX_ROWS]

    # Clip each cell to avoid huge payloads (Files/Photos/Pinmap etc.)
    safe_rows: List[List[str]] = []
    for r in rows:
        safe_rows.append([_clip(x) for x in r])

    # Append in batches to avoid single massive request -> BrokenPipe
    for batch in _chunked(safe_rows, LOG_DETAIL_BATCH):
        append_rows(svc, LOG_SHEET_ID, LOG_DETAILS_TAB, batch)

def log_summary(svc, row: List[str]):
    if LOG_ENABLED and row:
        append_rows(svc, LOG_SHEET_ID, LOG_SUMMARY_TAB, [row])

# ========================
# Glide helpers
# ========================
def glide_query_rows() -> List[Dict]:
    url = f"{GLIDE_BASE}/api/function/queryTables"

    all_rows: List[Dict] = []
    start_at = None

    while True:
        q = {"tableName": GLIDE_TABLE_NAME, "utc": True}
        if start_at:
            q["startAt"] = start_at

        body = {"appID": GLIDE_APP_ID, "queries": [q]}
        r = requests.post(url, headers={"Authorization": f"Bearer {GLIDE_TOKEN}"}, json=body, timeout=120)
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

        # fallback (rare)
        return []
    
def glide_add_row(column_values: Dict[str, str]):
    url = f"{GLIDE_BASE}/api/function/mutateTables"
    body = {"appID": GLIDE_APP_ID, "mutations": [{
        "kind": "add-row-to-table",
        "tableName": GLIDE_TABLE_NAME,
        "columnValues": column_values
    }]}
    r = requests.post(url, headers={"Authorization": f"Bearer {GLIDE_TOKEN}"}, json=body, timeout=120)
    r.raise_for_status()
    return r.json()

def glide_set_columns(row_id: str, column_values: Dict[str, str]):
    url = f"{GLIDE_BASE}/api/function/mutateTables"
    body = {"appID": GLIDE_APP_ID, "mutations": [{
        "kind": "set-columns-in-row",
        "tableName": GLIDE_TABLE_NAME,
        "rowID": row_id,
        "columnValues": column_values
    }]}
    r = requests.post(url, headers={"Authorization": f"Bearer {GLIDE_TOKEN}"}, json=body, timeout=120)
    r.raise_for_status()
    return r.json()

def glide_rowid(g: Dict) -> str:
    return str(g.get("$rowID") or g.get("rowID") or "").strip()

# ========================
# Timestamp parsing
# ========================
def parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    t = str(s).strip()
    # try ISO
    try:
        if t.endswith("Z"):
            return datetime.fromisoformat(t.replace("Z","+00:00"))
        if "T" in t:
            return datetime.fromisoformat(t)
    except Exception:
        pass
    # common sheet formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"):
        try:
            return datetime.strptime(t, fmt)
        except Exception:
            continue
    return None

# ========================
# Main
# ========================
def run():
    svc = sheets_service()
    ensure_log_tabs(svc)

    run_id = uuid.uuid4().hex[:8]
    ts = now_ist()

    ensure_tab(svc, SHEET_ID, SHEET_TAB)
    header = get_header(svc, SHEET_ID, SHEET_TAB)
    header = [h for h in header if h is not None and str(h).strip() != ""]

    required_headers = [SHEET_KEY_HEADER, SHEET_ROWID_HEADER, SHEET_UPDATED_AT_HEADER, SHEET_UPDATED_BY_HEADER] + list(MAPPING.keys())
    missing = [h for h in required_headers if h not in header]
    if missing:
        header = header + missing
        set_header(svc, SHEET_ID, SHEET_TAB, header)

    idx = {h:i for i,h in enumerate(header)}
    rows = read_all_rows(svc, SHEET_ID, SHEET_TAB, len(header))
    rows_padded = [pad_row(r, len(header)) for r in rows]

    # Build sheet map by CCP ID
    kpos = idx[SHEET_KEY_HEADER]
    rowid_pos = idx[SHEET_ROWID_HEADER]
    upd_at_pos = idx[SHEET_UPDATED_AT_HEADER]
    upd_by_pos = idx[SHEET_UPDATED_BY_HEADER]

    sheet_by_id: Dict[str, Tuple[int, List[str]]] = {}
    for i, r in enumerate(rows_padded, start=2):
        key = r[kpos].strip()
        if key and key not in sheet_by_id:
            sheet_by_id[key] = (i, r)

    # Build glide map by CCP ID (from mapping)
    glide_rows = glide_query_rows()
    glide_by_id: Dict[str, Dict] = {}
    ccp_id_glide_col = MAPPING.get(SHEET_KEY_HEADER)
    if not ccp_id_glide_col:
        raise SystemExit("CCP_GLIDE_COLUMNS_JSON must include mapping for 'CCP ID' (sheet key)")

    for g in glide_rows:
        gid = str(g.get(ccp_id_glide_col, "")).strip()
        if gid and gid not in glide_by_id:
            glide_by_id[gid] = g

    details: List[List[str]] = []
    updates_sheet: List[Tuple[int,int,str]] = []
    sheet_appends: List[List[str]] = []
    glide_added = 0
    glide_updated = 0

    # Helper: create payload from sheet row (only mapped headers)
    def payload_from_sheet(r: List[str]) -> Dict[str,str]:
        out = {}
        for sh, gc in MAPPING.items():
            pos = idx.get(sh)
            if pos is None:
                continue
            out[gc] = r[pos] if pos < len(r) else ""
        return out

    # Helper: create sheet row from glide row
    def sheet_row_from_glide(g: Dict) -> List[str]:
        r = [""] * len(header)
        # key
        r[kpos] = str(g.get(ccp_id_glide_col, "")).strip()
        # pointer
        r[rowid_pos] = glide_rowid(g)
        # updated fields
        r[upd_at_pos] = str(g.get(GLIDE_UPDATED_AT_COL, "") or "")
        r[upd_by_pos] = str(g.get(GLIDE_UPDATED_BY_COL, "") or "")
        # mapped fields
        for sh, gc in MAPPING.items():
            if sh in idx:
                r[idx[sh]] = "" if g.get(gc) is None else str(g.get(gc))
        return r

    # A) Glide-only rows -> append to sheet
    for gid, g in glide_by_id.items():
        if gid not in sheet_by_id:
            newr = sheet_row_from_glide(g)
            sheet_appends.append(newr)
            details.append([ts, run_id, "append_sheet", gid, "sheet", "(row)", "(blank)", json.dumps(newr, ensure_ascii=False)])

    # B) Sheet rows: add/update glide, or pull glide->sheet based on UpdatedAt
    for gid, (sheet_rnum, sr) in sheet_by_id.items():
        g = glide_by_id.get(gid)

        sr_upd = sr[upd_at_pos].strip()
        gr_upd = str(g.get(GLIDE_UPDATED_AT_COL, "")).strip() if g else ""

        sdt = parse_dt(sr_upd)
        gdt = parse_dt(gr_upd)

        if not g:
            # add to glide
            payload = payload_from_sheet(sr)
            glide_add_row(payload)
            glide_added += 1
            details.append([ts, run_id, "add_glide", gid, "glide", "(row)", "(blank)", json.dumps(payload, ensure_ascii=False)])
            continue

        # decide direction
        direction = None  # "sheet_to_glide" | "glide_to_sheet"
        if sdt and gdt:
            if sdt > gdt:
                direction = "sheet_to_glide"
            elif gdt > sdt:
                direction = "glide_to_sheet"
            else:
                direction = "none"
        elif sdt and not gdt:
            direction = "sheet_to_glide"
        elif gdt and not sdt:
            direction = "glide_to_sheet"
        else:
            # no usable timestamps => policy
            direction = "sheet_to_glide" if CONFLICT_WINNER == "sheet" else "glide_to_sheet"

        # always repair pointer in sheet if missing
        gid_rowid = glide_rowid(g)
        if gid_rowid and sr[rowid_pos].strip() != gid_rowid:
            updates_sheet.append((sheet_rnum, rowid_pos+1, gid_rowid))
            details.append([ts, run_id, "update_sheet", gid, "sheet", SHEET_ROWID_HEADER, sr[rowid_pos], gid_rowid])

        if direction == "sheet_to_glide":
            # update glide only if diff on mapped columns
            payload = {}
            for sh, gc in MAPPING.items():
                pos = idx.get(sh)
                if pos is None:
                    continue
                sv = sr[pos] if pos < len(sr) else ""
                gv = "" if g.get(gc) is None else str(g.get(gc))
                if str(sv) != str(gv):
                    payload[gc] = sv
                    details.append([ts, run_id, "update_glide", gid, "glide", sh, gv, sv])

            if payload:
                glide_set_columns(gid_rowid, payload)
                glide_updated += 1

        elif direction == "glide_to_sheet":
            # update sheet cells only where diff on mapped headers + updated fields
            def set_if_diff(col_header: str, newv: str):
                pos = idx.get(col_header)
                if pos is None:
                    return
                oldv = sr[pos] if pos < len(sr) else ""
                if str(oldv) != str(newv):
                    updates_sheet.append((sheet_rnum, pos+1, newv))
                    details.append([ts, run_id, "update_sheet", gid, "sheet", col_header, oldv, newv])

            # updated fields
            set_if_diff(SHEET_UPDATED_AT_HEADER, str(g.get(GLIDE_UPDATED_AT_COL, "") or ""))
            set_if_diff(SHEET_UPDATED_BY_HEADER, str(g.get(GLIDE_UPDATED_BY_COL, "") or ""))

            # mapped fields
            for sh, gc in MAPPING.items():
                set_if_diff(sh, "" if g.get(gc) is None else str(g.get(gc)))

    # Apply writes
    if sheet_appends:
        append_rows(svc, SHEET_ID, SHEET_TAB, sheet_appends)
    if updates_sheet:
        batch_update_cells(svc, SHEET_ID, SHEET_TAB, updates_sheet)

    # Logs
    log_details(svc, details)

    log_summary(svc, [
        ts, run_id, SCRIPT_VERSION, "ccp_two_way", SHEET_TAB, GLIDE_TABLE_NAME,
        str(len(rows_padded)), str(len(glide_rows)),
        str(len(sheet_appends)), str(len(updates_sheet)),
        str(glide_added), str(glide_updated),
        "ok", ""
    ])

    print(json.dumps({
        "ok": True,
        "sheet_appended": len(sheet_appends),
        "sheet_updated_cells": len(updates_sheet),
        "glide_added": glide_added,
        "glide_updated": glide_updated
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
                ts, run_id, SCRIPT_VERSION, "ccp_two_way", SHEET_TAB, GLIDE_TABLE_NAME,
                "?", "?", "0", "0", "0", "0", "error", str(e)
            ])
        except Exception:
            pass
        raise