import os, json, hashlib, uuid, datetime
from typing import List, Dict, Tuple
import requests
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timezone
import pytz
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ========================
# Load .env from this file
# ========================
ENV_PATH = Path(__file__).resolve().with_name('.env')
load_dotenv(dotenv_path=ENV_PATH)

def need(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise SystemExit(f"Missing required env: {name}. Did you create {ENV_PATH} and fill it?")
    return v

# ================
# Required ENV VARS
# ================
SHEET_ID     = need("GOOGLE_SHEET_ID")
CREDS_JSON   = json.loads(need("GOOGLE_CREDENTIALS_JSON"))

# Behavior / naming
UNIQUE_KEY   = os.getenv("UNIQUE_KEY", "$rowID")            # Glide primary key
SHEET_NAME   = os.getenv("SHEET_NAME", "GlideMirror")
META_SHEET   = os.getenv("META_SHEET", "_meta")
WRITE_MODE   = os.getenv("WRITE_MODE", "delta").lower()     # "delta" | "full"

# Mapped columns only (GlideKey -> SheetHeader)
SELECT_COLUMNS_STR = os.getenv("SELECT_COLUMNS", "{}")
try:
    SELECT_COLUMNS: Dict[str, str] = json.loads(SELECT_COLUMNS_STR) if SELECT_COLUMNS_STR.strip() else {}
except Exception as e:
    raise SystemExit(f"Invalid SELECT_COLUMNS JSON: {e}")

# Derived column: "ID" = "Project name - Part name" (disable by setting to "")
DERIVED_ID_HEADER = os.getenv("DERIVED_ID_HEADER", "ID").strip()

# -------- Logging (NEW) --------
LOG_ENABLED          = os.getenv("LOG_ENABLED", "true").strip().lower() in ("1","true","yes","y")
LOG_SHEET_ID         = os.getenv("LOG_SHEET_ID", "").strip()                  # separate spreadsheet for logs
LOG_DETAILS_TAB      = os.getenv("LOG_DETAILS_TAB", "change_details").strip() # per-change rows
LOG_SUMMARY_TAB      = os.getenv("LOG_SUMMARY_TAB", "run_summary").strip()    # exactly one row per run
SCRIPT_VERSION       = os.getenv("SCRIPT_VERSION", "").strip()                # optional
# --------------------------------

# Glide access (read-only)
GLIDE_MODE   = os.getenv("GLIDE_MODE", "legacy").lower()          # "advanced" | "legacy"
GLIDE_BASE   = os.getenv("GLIDE_BASE_URL", "https://api.glideapp.io").rstrip("/")
GLIDE_TOKEN  = need("GLIDE_TOKEN")

# Advanced only
TABLE_ID     = os.getenv("GLIDE_TABLE_ID", "")

# Legacy only
GLIDE_APP_ID     = os.getenv("GLIDE_APP_ID", "")
GLIDE_TABLE_NAME = os.getenv("GLIDE_TABLE_NAME", "")

# ======================
# Google Sheets utilities
# ======================
def sheets_service():
    creds = Credentials.from_service_account_info(
        CREDS_JSON,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)

def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def ensure_sheet_exists(svc, spreadsheet_id: str, name: str):
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    if any(sh["properties"]["title"] == name for sh in meta.get("sheets", [])):
        return
    svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": name}}}]}
    ).execute()

def get_sheet_id(svc, spreadsheet_id: str, title: str) -> int:
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sh in meta.get("sheets", []):
        if sh["properties"]["title"] == title:
            return sh["properties"]["sheetId"]
    raise RuntimeError(f"Sheet '{title}' not found")

def get_header(svc, spreadsheet_id: str, name: str) -> List[str]:
    res = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"'{name}'!1:1"
    ).execute()
    return [str(x).strip() for x in (res.get("values", [[]])[0]) if str(x).strip()]

def set_header_row_values(svc, spreadsheet_id: str, name: str, header: List[str]):
    end = col_letter(max(1, len(header)))
    svc.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{name}'!A1:{end}1",
        valueInputOption="RAW",
        body={"values": [header if header else ['']]}
    ).execute()

def read_body(svc, spreadsheet_id: str, name: str, cols: int) -> List[List[str]]:
    end = col_letter(max(1, cols))
    res = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"'{name}'!A2:{end}"
    ).execute()
    return res.get("values", [])

def pad_row(row: List[str], width: int) -> List[str]:
    if len(row) >= width:
        return row[:width]
    return row + [""] * (width - len(row))

def batch_update_cells(svc, spreadsheet_id: str, name: str, updates: List[Tuple[int,int,str]]):
    """
    updates: list of (row_1based, col_1based, value)
    Writes only the exact cells (no ranges spanning unmapped columns).
    """
    if not updates:
        return
    by_row: Dict[int, List[Tuple[int,str]]] = {}
    for r, c, v in updates:
        by_row.setdefault(r, []).append((c, v))

    data = []
    for r, cols_vals in by_row.items():
        for c, v in cols_vals:
            colL = col_letter(c)
            data.append({
                "range": f"'{name}'!{colL}{r}:{colL}{r}",
                "values": [[v]]
            })

    if not data:
        return
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"valueInputOption": "RAW", "data": data}
    ).execute()

def append_rows(svc, spreadsheet_id: str, name: str, rows: List[List[str]]):
    if not rows:
        return
    svc.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"'{name}'!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()

def delete_rows_by_numbers(svc, spreadsheet_id: str, name: str, row_nums_1based: List[int]):
    if not row_nums_1based:
        return
    sheet_id = get_sheet_id(svc, spreadsheet_id, name)
    reqs = []
    for rn in sorted(row_nums_1based, reverse=True):
        reqs.append({
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": rn - 1,
                    "endIndex": rn
                }
            }
        })
    svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()

# ============ META (hash) ============
def get_meta_map(svc) -> Dict[str, str]:
    ensure_sheet_exists(svc, SHEET_ID, META_SHEET)
    res = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{META_SHEET}'!A:B"
    ).execute()
    m: Dict[str, str] = {}
    for row in res.get("values", []):
        if row and row[0]:
            m[row[0]] = row[1] if len(row) > 1 else ""
    return m

def set_meta_map(svc, m: Dict[str, str]):
    ensure_sheet_exists(svc, SHEET_ID, META_SHEET)
    values = [[k, v] for k, v in m.items()] or [["", ""]]
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"'{META_SHEET}'!A1:B{max(1, len(values))}",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

# =============================
# Glide fetchers (read-only)
# =============================
def _advanced_fetch():
    out = []
    cont = ""
    while True:
        url = f"{GLIDE_BASE}/tables/{requests.utils.quote(TABLE_ID, safe='')}/rows"
        if cont:
            url += f"?continuation={cont}"
        r = requests.get(url, headers={"Authorization": f"Bearer {GLIDE_TOKEN}"}, timeout=120)
        r.raise_for_status()
        j = r.json()
        out += j.get("data", [])
        cont = j.get("continuation", "")
        if not cont:
            break
    return out

def _legacy_fetch():
    url = f"{GLIDE_BASE}/api/function/queryTables"
    body = {"appID": GLIDE_APP_ID, "queries": [{"tableName": GLIDE_TABLE_NAME, "utc": True}]}
    r = requests.post(url, headers={"Authorization": f"Bearer {GLIDE_TOKEN}"}, json=body, timeout=120)
    r.raise_for_status()
    j = r.json()

    if isinstance(j, dict):
        containers = j.get("data") or j.get("result") or j.get("tables") or j.get("Results") or j.get("response") or []
    else:
        containers = j

    rows = []
    if isinstance(containers, list):
        if not containers:
            rows = []
        else:
            first = containers[0]
            if isinstance(first, dict) and not any(k in first for k in ("rows", "data", "items", "records")):
                rows = containers
            else:
                for t in containers:
                    if isinstance(t, dict):
                        cand = t.get("rows") or t.get("data") or t.get("items") or t.get("records")
                        if isinstance(cand, list):
                            rows = cand
                            break
    elif isinstance(containers, dict):
        cand = containers.get("rows") or containers.get("data") or containers.get("items") or containers.get("records")
        if isinstance(cand, list):
            rows = cand
    return rows

def glide_fetch_all() -> List[Dict]:
    if GLIDE_MODE == "advanced":
        if not TABLE_ID:
            raise RuntimeError("GLIDE_TABLE_ID is required for advanced mode.")
        return _advanced_fetch()
    if not GLIDE_APP_ID or not GLIDE_TABLE_NAME:
        raise RuntimeError("GLIDE_APP_ID and GLIDE_TABLE_NAME are required for legacy mode.")
    return _legacy_fetch()

# ======================
# Normalization
# ======================
def norm(v):
    if v is None:
        return ""
    if isinstance(v, list):
        return ",".join(map(str, v))
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    return str(v).strip()

# ---- Date formatting (existing) ----
DATE_MM_DD_HEADERS = {"Date created at"}  # headers to force mm-dd-yyyy

def to_mmddyyyy(s: str) -> str:
    if not s:
        return ""
    cand = s.strip()
    try:
        if cand.endswith("Z"):
            return datetime.fromisoformat(cand.replace("Z", "+00:00")).strftime("%m-%d-%Y")
        if "T" in cand:
            return datetime.fromisoformat(cand).strftime("%m-%d-%Y")
    except Exception:
        pass
    for f in ("%Y-%m-%d","%m/%d/%Y","%d/%m/%Y","%Y/%m/%d"):
        try:
            return datetime.strptime(cand, f).strftime("%m-%d-%Y")
        except Exception:
            continue
    return cand
# -----------------------------------

# =========
# Mapped header model (+ optional derived ID)
# =========
def build_selected_headers() -> Tuple[List[str], Dict[str, str]]:
    if not SELECT_COLUMNS:
        raise SystemExit("SELECT_COLUMNS is empty. Populate it in .env.")
    items = list(SELECT_COLUMNS.items())
    items.sort(key=lambda kv: (0 if kv[0] == UNIQUE_KEY else 1, kv[1].lower()))
    headers = [kv[1] for kv in items]
    if DERIVED_ID_HEADER and DERIVED_ID_HEADER not in headers:
        headers.append(DERIVED_ID_HEADER)
    g2s = dict(items)
    return headers, g2s

# =========
# Hash (only mapped columns, no cap)
# =========
def compute_hash_selected(glide_rows: List[Dict], selected_headers: List[str], g2s: Dict[str, str]) -> str:
    s2g = {v: k for k, v in g2s.items()}

    def derived_id(obj: Dict) -> str:
        if not DERIVED_ID_HEADER:
            return ""
        proj = norm(obj.get(s2g.get("Project name", ""), ""))
        part_name = norm(obj.get(s2g.get("Part name", ""), ""))
        return f"{proj} - {part_name}" if (proj or part_name) else ""

    ordered_headers = [h for h in selected_headers if h != DERIVED_ID_HEADER]
    blob_parts = [f"N={len(glide_rows)}",
                  f"H={'|'.join(ordered_headers + ([DERIVED_ID_HEADER] if DERIVED_ID_HEADER else []))}"]
    for r in glide_rows:
        key = norm(r.get(UNIQUE_KEY, ""))
        row_vals = []
        for h in ordered_headers:
            gk = s2g.get(h, "")
            row_vals.append(norm(r.get(gk, "")))
        if DERIVED_ID_HEADER:
            row_vals.append(derived_id(r))
        blob_parts.append(key + "|" + "|".join(row_vals))
    blob = "\n".join(blob_parts)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()

# =========
# Headers (UNION: keep existing extras, never delete/reorder; just append missing mapped headers)
# =========
def ensure_headers_union_preserve_extras(svc, selected_headers: List[str]) -> List[str]:
    current = get_header(svc, SHEET_ID, SHEET_NAME)
    if not current:
        set_header_row_values(svc, SHEET_ID, SHEET_NAME, selected_headers)
        return selected_headers[:]
    missing = [h for h in selected_headers if h not in current]
    if missing:
        set_header_row_values(svc, SHEET_ID, SHEET_NAME, current + missing)
        current = current + missing
    return current

# =========
# Logging helpers (NEW) - Modified to use IST
# =========
DETAIL_COLUMNS = [
    "timestamp_ist","run_id","action","row_id","sheet_row_num",
    "column_name","old_value","new_value"
]
SUMMARY_COLUMNS = [
    "timestamp_ist","run_id","script_version","sheet_name","glide_mode",
    "rows_in_glide","rows_in_sheet_before","rows_in_sheet_after",
    "inserted","updated_cells","deleted","duplicates_deleted","hash_prev","hash_new","result","error_message"
]

def ensure_log_headers(svc):
    if not LOG_ENABLED or not LOG_SHEET_ID:
        return
    ensure_sheet_exists(svc, LOG_SHEET_ID, LOG_DETAILS_TAB)
    cur = get_header(svc, LOG_SHEET_ID, LOG_DETAILS_TAB)
    if cur != DETAIL_COLUMNS:
        set_header_row_values(svc, LOG_SHEET_ID, LOG_DETAILS_TAB, DETAIL_COLUMNS)
    ensure_sheet_exists(svc, LOG_SHEET_ID, LOG_SUMMARY_TAB)
    cur2 = get_header(svc, LOG_SHEET_ID, LOG_SUMMARY_TAB)
    if cur2 != SUMMARY_COLUMNS:
        set_header_row_values(svc, LOG_SHEET_ID, LOG_SUMMARY_TAB, SUMMARY_COLUMNS)

def append_detail_logs(svc, rows: List[List[str]]):
    if LOG_ENABLED and LOG_SHEET_ID and rows:
        append_rows(svc, LOG_SHEET_ID, LOG_DETAILS_TAB, rows)

def append_run_summary(svc, row: List[str]):
    if LOG_ENABLED and LOG_SHEET_ID and row:
        append_rows(svc, LOG_SHEET_ID, LOG_SUMMARY_TAB, [row])

def now_ist_iso():
    """Return current time in IST timezone as ISO string without timezone suffix"""
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist).replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')

# ---- NEW: helpers to snapshot mapped data into a compact JSON string ----
def mapped_snapshot_from_values(values: List[str], cur_index: Dict[str,int], selected_headers: List[str]) -> str:
    snap = {}
    for sh in selected_headers:
        idx = cur_index.get(sh)
        if idx is not None:
            snap[sh] = values[idx] if idx < len(values) else ""
    # compact JSON, no spaces
    return json.dumps(snap, ensure_ascii=False, separators=(",",":"))

# =========
# MIRROR (cell-level; update only mapped columns; preserve unmapped)
# =========
def mirror(force=False, dry=False, inspect=False):
    if inspect:
        rows = glide_fetch_all()
        cols = sorted({k for r in rows for k in r.keys()}) if rows else []
        print("Available columns:", cols)
        if rows:
            print("Sample row:", json.dumps(rows[0], indent=2, ensure_ascii=False))
        return {"inspected": True, "columns": cols, "sample_present": bool(rows)}

    svc = sheets_service()
    ensure_sheet_exists(svc, SHEET_ID, SHEET_NAME)
    ensure_log_headers(svc)

    g_rows = glide_fetch_all()

    selected_headers, g2s = build_selected_headers()
    current_headers = ensure_headers_union_preserve_extras(svc, selected_headers)
    cur_index = {h: i for i, h in enumerate(current_headers)}

    meta_key = f"hash:{GLIDE_MODE}:{TABLE_ID or GLIDE_TABLE_NAME}:{SHEET_NAME}"
    meta = get_meta_map(svc)
    prev_hash = meta.get(meta_key, "")
    new_hash = compute_hash_selected(g_rows, selected_headers, g2s) if g_rows else "EMPTY"

    run_id = uuid.uuid4().hex[:8]
    ts = now_ist_iso()

    body_before = read_body(svc, SHEET_ID, SHEET_NAME, len(current_headers))
    rows_in_sheet_before = len(body_before)
    rows_in_glide = len(g_rows)

    # Handle Glide empty
    if not g_rows:
        if dry:
            return {"skipped": False, "dry": True, "inserted": 0, "updated_cells": 0, "deleted": "ALL"}
        detail_logs = []
        key_header = SELECT_COLUMNS.get(UNIQUE_KEY, "RowID")
        if key_header not in current_headers and selected_headers:
            key_header = selected_headers[0]
        key_idx = cur_index.get(key_header, 0)
        rn = 1
        for row in body_before:
            rn += 1
            prow = pad_row(row, len(current_headers))
            rid = prow[key_idx] if key_idx < len(prow) else ""
            # old_value = snapshot of mapped columns
            old_json = mapped_snapshot_from_values(prow, cur_index, selected_headers)
            detail_logs.append([ts, run_id, "delete", rid, str(rn), "(entire row)", old_json, "(blank)"])
        if detail_logs:
            append_detail_logs(svc, detail_logs)
        if rows_in_sheet_before:
            delete_rows_by_numbers(svc, SHEET_ID, SHEET_NAME, list(range(2, 2 + rows_in_sheet_before)))

        rows_in_sheet_after = 0
        append_run_summary(svc, [
            ts, run_id, SCRIPT_VERSION, SHEET_NAME, GLIDE_MODE,
            str(rows_in_glide), str(rows_in_sheet_before), str(rows_in_sheet_after),
            "0", "0", str(rows_in_sheet_before), "0", prev_hash, new_hash, "ok", ""
        ])
        meta[meta_key] = new_hash
        set_meta_map(svc, meta)
        return {"skipped": False, "inserted": 0, "updated_cells": 0, "deleted": rows_in_sheet_before, "verified": True}

    # If no change and not forced -> summary with "skipped"
    if not force and prev_hash == new_hash:
        if not dry:
            append_run_summary(svc, [
                ts, run_id, SCRIPT_VERSION, SHEET_NAME, GLIDE_MODE,
                str(rows_in_glide), str(rows_in_sheet_before), str(rows_in_sheet_before),
                "0", "0", "0", "0", prev_hash, new_hash, "skipped", ""
            ])
        return {"skipped": True, "reason": "hash-match", "inserted": 0, "updated_cells": 0, "deleted": 0, "verified": True}

    s2g = {v: k for k, v in g2s.items()}

    key_header = SELECT_COLUMNS.get(UNIQUE_KEY, "RowID")
    if key_header not in current_headers:
        key_header = selected_headers[0]
    key_col_idx = cur_index[key_header]

    # NEW: Track duplicate rows in the sheet and mark them for deletion
    sheet_map: Dict[str, List[str]] = {}
    rownum_by_key: Dict[str, int] = {}
    duplicate_rows_to_delete: List[int] = []
    rn = 1
    for row in body_before:
        rn += 1
        prow = pad_row(row, len(current_headers))
        k = prow[key_col_idx] if key_col_idx < len(prow) else ""
        if k:
            sk = str(k)
            if sk in sheet_map:
                # This is a duplicate - mark for deletion
                duplicate_rows_to_delete.append(rn)
            else:
                # First occurrence - keep it
                sheet_map[sk] = [str(x) for x in prow]
                rownum_by_key[sk] = rn

    def derived_id_from_obj(obj: Dict) -> str:
        if not DERIVED_ID_HEADER:
            return ""
        proj = norm(obj.get(s2g.get("Project name", ""), ""))
        part_name = norm(obj.get(s2g.get("Part name", ""), ""))
        return f"{proj} - {part_name}" if (proj or part_name) else ""

    def row_mapped_into_sheet_width(obj: Dict) -> List[str]:
        row_vals = [""] * len(current_headers)
        for sh in selected_headers:
            if sh == DERIVED_ID_HEADER:
                if sh in cur_index:
                    row_vals[cur_index[sh]] = derived_id_from_obj(obj)
                continue
            if sh in cur_index:
                gk = s2g.get(sh, None)
                val = norm(obj.get(gk, "")) if gk else ""
                if sh in DATE_MM_DD_HEADERS and val:
                    val = to_mmddyyyy(val)
                row_vals[cur_index[sh]] = val
        return row_vals

    g_map: Dict[str, List[str]] = {}
    g_order: List[str] = []
    for obj in g_rows:
        k = obj.get(UNIQUE_KEY, "")
        if not k:
            continue
        sk = str(k)
        g_order.append(sk)
        g_map[sk] = row_mapped_into_sheet_width(obj)

    updated_cells: List[Tuple[int,int,str]] = []
    rows_to_append: List[List[str]] = []
    rows_to_delete: List[int] = []
    detail_logs: List[List[str]] = []

    mapped_positions = [cur_index[h] for h in selected_headers if h in cur_index]

    # Add duplicate rows to deletion list and log them
    for dup_row_num in duplicate_rows_to_delete:
        rows_to_delete.append(dup_row_num)
        detail_logs.append([ts, run_id, "delete_duplicate", "duplicate", str(dup_row_num), "(duplicate row)", "(duplicate)", "(blank)"])

    seen = set()
    for key, sheet_vals in sheet_map.items():
        new_vals = g_map.get(key)
        if not new_vals:
            rows_to_delete.append(rownum_by_key[key])
            # delete: old_value = JSON snapshot of mapped columns from current sheet row
            old_json = mapped_snapshot_from_values(sheet_vals, cur_index, selected_headers)
            detail_logs.append([ts, run_id, "delete", key, str(rownum_by_key[key]), "(entire row)", old_json, "(blank)"])
            continue
        seen.add(key)
        for ci in mapped_positions:
            oldv = sheet_vals[ci] if ci < len(sheet_vals) else ""
            newv = new_vals[ci]
            if oldv != newv:
                updated_cells.append((rownum_by_key[key], ci + 1, newv))
                detail_logs.append([ts, run_id, "update", key, str(rownum_by_key[key]), current_headers[ci], oldv, newv])

    current_row_count = len(body_before)
    for key in g_order:
        if key in seen:
            continue
        rows_to_append.append(g_map[key])
        predicted_rownum = 2 + current_row_count + len(rows_to_append)
        # insert: new_value = JSON snapshot of mapped columns for the new row
        new_json = mapped_snapshot_from_values(g_map[key], cur_index, selected_headers)
        detail_logs.append([ts, run_id, "insert", key, str(predicted_rownum), "(all mapped)", "(blank)", new_json])

    if dry:
        return {
            "skipped": False,
            "dry": True,
            "inserted": len(rows_to_append),
            "updated_cells": len(updated_cells),
            "deleted": len(rows_to_delete),
            "duplicates_found": len(duplicate_rows_to_delete)
        }

    if WRITE_MODE == "delta":
        batch_update_cells(svc, SHEET_ID, SHEET_NAME, updated_cells)
        delete_rows_by_numbers(svc, SHEET_ID, SHEET_NAME, rows_to_delete)
        append_rows(svc, SHEET_ID, SHEET_NAME, rows_to_append)
    else:
        end = col_letter(len(current_headers))
        svc.spreadsheets().values().clear(spreadsheetId=SHEET_ID, range=f"'{SHEET_NAME}'!A:ZZZ").execute()
        set_header_row_values(svc, SHEET_ID, SHEET_NAME, current_headers)
        if g_order:
            CHUNK = 5000
            final_rows = [g_map[k] for k in g_order]
            for i in range(0, len(final_rows), CHUNK):
                part = final_rows[i:i+CHUNK]
                r1 = 2 + i
                r2 = r1 + len(part) - 1
                svc.spreadsheets().values().update(
                    spreadsheetId=SHEET_ID,
                    range=f"'{SHEET_NAME}'!A{r1}:{end}{r2}",
                    valueInputOption="RAW",
                    body={"values": part}
                ).execute()

    append_detail_logs(svc, detail_logs)

    inserted = len(rows_to_append)
    updated_cells_count = len(updated_cells)
    deleted = len(rows_to_delete)
    rows_in_sheet_after = rows_in_sheet_before - deleted + inserted
    append_run_summary(svc, [
        ts, run_id, SCRIPT_VERSION, SHEET_NAME, GLIDE_MODE,
        str(rows_in_glide), str(rows_in_sheet_before), str(rows_in_sheet_after),
        str(inserted), str(updated_cells_count), str(deleted), str(len(duplicate_rows_to_delete)), prev_hash, new_hash, "ok", ""
    ])

    meta[meta_key] = new_hash
    set_meta_map(svc, meta)

    return {
        "skipped": False,
        "inserted": inserted,
        "updated_cells": updated_cells_count,
        "deleted": deleted,
        "duplicates_removed": len(duplicate_rows_to_delete),
        "verified": True
    }

# ====== CLI ======
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Glide → Google Sheet (union headers, update only mapped columns, cell-level) + dual logging")
    p.add_argument("--force", action="store_true", help="Ignore hash short-circuit")
    p.add_argument("--dry", action="store_true", help="Show planned changes only; no writes or logs")
    p.add_argument("--inspect", action="store_true", help="Print available Glide columns; no writes")
    args = p.parse_args()

    res = mirror(force=args.force, dry=args.dry, inspect=args.inspect)
    print(json.dumps(res, indent=2, ensure_ascii=False))
