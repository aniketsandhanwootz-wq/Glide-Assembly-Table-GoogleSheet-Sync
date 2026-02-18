# jobs/02_project_glide_to_sheet.py
from __future__ import annotations

import os
import json
import hashlib
import uuid
from typing import List, Dict, Tuple, Any
from pathlib import Path
from datetime import datetime

# --- import path fix (Render cron) ---
import sys
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# --- end fix ---

from zai_webhook import emit_zai_event
import pytz
import requests
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ========================
# Load .env (repo root)
# ========================
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

def need(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise SystemExit(f"Missing required env: {name} (in {ENV_PATH})")
    return v

def opt(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()

# ========================
# Global config
# ========================
SHEET_ID     = need("GOOGLE_SHEET_ID")
CREDS_JSON   = json.loads(need("GOOGLE_CREDENTIALS_JSON"))

META_SHEET   = opt("META_SHEET", "_meta")

# Logging: keep minimal by default
LOG_ENABLED        = opt("LOG_ENABLED", "true").lower() in ("1", "true", "yes", "y")
LOG_DETAILS        = opt("LOG_DETAILS", "false").lower() in ("1", "true", "yes", "y")  # default OFF
LOG_SHEET_ID       = opt("LOG_SHEET_ID", "") or SHEET_ID
SCRIPT_VERSION     = opt("SCRIPT_VERSION", "")
RUN_LOG_TAB        = opt("PROJ_LOG_SUMMARY_TAB", "proj_run_summary")
DETAILS_LOG_TAB    = opt("PROJ_LOG_DETAILS_TAB", "proj_change_details")

# ========================
# Job config (Project)
# ========================
SHEET_NAME        = need("PROJ_SHEET_NAME")
WRITE_MODE        = opt("PROJ_WRITE_MODE", "delta").lower()  # delta | full
UNIQUE_KEY        = opt("PROJ_UNIQUE_KEY", "$rowID")
DERIVED_ID_HEADER = opt("PROJ_DERIVED_ID_HEADER", "ID")

# Trigger config: fire PROJECT_UPDATED when status becomes mfg
PROJ_MFG_STATUS_HEADER = opt("PROJ_MFG_STATUS_HEADER", "Status_assembly")
PROJ_MFG_STATUS_VALUE  = opt("PROJ_MFG_STATUS_VALUE", "mfg").strip().lower()
# Optional: make derived-id fields explicit (recommended)
DERIVED_ID_PROJECT_HEADER = opt("PROJ_DERIVED_ID_PROJECT_HEADER", "")  # e.g. "Project"
DERIVED_ID_PART_HEADER    = opt("PROJ_DERIVED_ID_PART_HEADER", "")     # e.g. "Name"

SELECT_COLUMNS_STR = need("PROJ_SELECT_COLUMNS")
try:
    # GlideKey -> SheetHeader
    SELECT_COLUMNS: Dict[str, str] = json.loads(SELECT_COLUMNS_STR)
except Exception as e:
    raise SystemExit(f"Invalid PROJ_SELECT_COLUMNS JSON: {e}")

# ========================
# Glide (legacy)
# ========================
GLIDE_BASE   = opt("GLIDE_BASE_URL", "https://api.glideapp.io").rstrip("/")
GLIDE_TOKEN  = need("GLIDE_TOKEN")
GLIDE_APP_ID = need("GLIDE_APP_ID")
GLIDE_TABLE_NAME = need("PROJ_GLIDE_TABLE_NAME")

GLIDE_DEBUG_RAW = opt("GLIDE_DEBUG_RAW", "false").lower() in ("1", "true", "yes", "y")
GLIDE_DEBUG_MAX_CHARS = int(opt("GLIDE_DEBUG_MAX_CHARS", "5000") or 5000)

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

def batch_update_cells(svc, spreadsheet_id: str, name: str, updates: List[Tuple[int, int, str]]):
    if not updates:
        return
    data = []
    for r, c, v in updates:
        colL = col_letter(c)
        data.append({"range": f"'{name}'!{colL}{r}:{colL}{r}", "values": [[v]]})
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
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": rn - 1, "endIndex": rn}
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
# Glide fetcher (legacy)
# =============================
def extract_glide_rows(payload) -> List[dict]:
    if payload is None:
        return []
    if isinstance(payload, list):
        if payload and isinstance(payload[0], dict) and ("rows" in payload[0]) and isinstance(payload[0]["rows"], list):
            return payload[0]["rows"]
        if payload and isinstance(payload[0], dict) and ("$rowID" in payload[0] or "rowID" in payload[0]):
            return payload
        return []
    if not isinstance(payload, dict):
        return []
    root_rows = payload.get("rows")
    if isinstance(root_rows, list):
        return root_rows
    data = payload.get("data") or payload.get("result") or payload.get("tables")
    if isinstance(data, dict):
        r = data.get("rows")
        if isinstance(r, list):
            return r
        if isinstance(data.get("data"), list):
            data = data.get("data")
    if isinstance(data, list) and data:
        for item in data:
            if isinstance(item, dict) and isinstance(item.get("rows"), list):
                return item["rows"]
    return []

def _normalize_glide_rowids(rows: List[Dict]) -> List[Dict]:
    out = []
    for r in rows or []:
        if isinstance(r, dict):
            if "$rowID" not in r and "rowID" in r and r.get("rowID"):
                r["$rowID"] = r.get("rowID")
        out.append(r)
    return out

def glide_fetch_all() -> List[Dict]:
    url = f"{GLIDE_BASE}/api/function/queryTables"
    body = {"appID": GLIDE_APP_ID, "queries": [{"tableName": GLIDE_TABLE_NAME, "utc": True}]}
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {GLIDE_TOKEN}"},
        json=body,
        timeout=120,
    )
    r.raise_for_status()
    j = r.json()

    if GLIDE_DEBUG_RAW:
        s = json.dumps(j, ensure_ascii=False, indent=2)
        print("\n--- GLIDE RAW RESPONSE (trimmed) ---")
        print(s[:GLIDE_DEBUG_MAX_CHARS])
        if len(s) > GLIDE_DEBUG_MAX_CHARS:
            print(f"...(trimmed {len(s)} chars total)")
        print("--- END GLIDE RAW RESPONSE ---\n")

    rows = extract_glide_rows(j)
    rows = _normalize_glide_rowids(rows)
    return rows

# ======================
# Normalization + dates
# ======================
def norm(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, list):
        # Keep parity with your earlier “working” script
        return ",".join(map(str, v))
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    return str(v).strip()

DATE_MM_DD_HEADERS = {"Date created at"}  # keep if you use it

def to_mmddyyyy(s: str) -> str:
    if not s:
        return ""
    cand = str(s).strip()
    try:
        if cand.endswith("Z"):
            return datetime.fromisoformat(cand.replace("Z", "+00:00")).strftime("%m-%d-%Y")
        if "T" in cand:
            return datetime.fromisoformat(cand).strftime("%m-%d-%Y")
    except Exception:
        pass
    for f in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(cand, f).strftime("%m-%d-%Y")
        except Exception:
            continue
    return cand

def now_ist_iso() -> str:
    ist = pytz.timezone("Asia/Kolkata")
    return datetime.now(ist).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

# =========
# Mapping model (+ derived ID)
# =========
def build_selected_headers() -> Tuple[List[str], Dict[str, str]]:
    if not SELECT_COLUMNS:
        raise SystemExit("PROJ_SELECT_COLUMNS is empty.")
    items = list(SELECT_COLUMNS.items())
    items.sort(key=lambda kv: (0 if kv[0] == UNIQUE_KEY else 1, kv[1].lower()))
    headers = [kv[1] for kv in items]
    if DERIVED_ID_HEADER and DERIVED_ID_HEADER not in headers:
        headers.append(DERIVED_ID_HEADER)
    return headers, dict(items)

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

def _pick_derived_fields(selected_headers: List[str]) -> Tuple[str, str]:
    # Prefer explicit env
    if DERIVED_ID_PROJECT_HEADER and DERIVED_ID_PART_HEADER:
        return DERIVED_ID_PROJECT_HEADER, DERIVED_ID_PART_HEADER

    # Otherwise infer common patterns from sheet headers
    sh_lower = {h.lower(): h for h in selected_headers}
    proj = ""
    part = ""

    for cand in ("project", "project name"):
        if cand in sh_lower:
            proj = sh_lower[cand]
            break
    for cand in ("name", "part", "part name"):
        if cand in sh_lower:
            part = sh_lower[cand]
            break

    # Last fallback: empty => derived will be blank
    return proj, part

def compute_hash_selected_from_rows(
    glide_rows: List[Dict],
    selected_headers: List[str],
    g2s: Dict[str, str],
) -> str:
    s2g = {v: k for k, v in g2s.items()}
    proj_h, part_h = _pick_derived_fields(selected_headers)

    def derived_id(obj: Dict) -> str:
        if not DERIVED_ID_HEADER:
            return ""
        proj = norm(obj.get(s2g.get(proj_h, ""), "")) if proj_h else ""
        part = norm(obj.get(s2g.get(part_h, ""), "")) if part_h else ""
        return f"{proj} - {part}" if (proj or part) else ""

    ordered = [h for h in selected_headers if h != DERIVED_ID_HEADER]
    blob_parts = [f"N={len(glide_rows)}", f"H={'|'.join(ordered + ([DERIVED_ID_HEADER] if DERIVED_ID_HEADER else []))}"]

    for r in glide_rows:
        key = norm(r.get(UNIQUE_KEY, ""))
        row_vals = []
        for h in ordered:
            gk = s2g.get(h, "")
            row_vals.append(norm(r.get(gk, "")))
        if DERIVED_ID_HEADER:
            row_vals.append(derived_id(r))
        blob_parts.append(key + "|" + "|".join(row_vals))

    return hashlib.sha256("\n".join(blob_parts).encode("utf-8")).hexdigest()

def compute_hash_selected_from_sheet(
    body_rows: List[List[str]],
    current_headers: List[str],
    selected_headers: List[str],
    key_col_idx_0: int,
) -> str:
    # Hash the sheet’s mapped view in the same “shape” as glide hash
    cur_index = {h: i for i, h in enumerate(current_headers)}

    legacy_id_header = DERIVED_ID_HEADER or "ID"
    if "ID" in current_headers:
        legacy_id_header = "ID"

    status_idx = cur_index.get(PROJ_MFG_STATUS_HEADER)
    id_idx = cur_index.get(legacy_id_header)
    ordered = [h for h in selected_headers if h != DERIVED_ID_HEADER]
    mapped_positions = [cur_index[h] for h in ordered if h in cur_index]
    derived_pos = cur_index.get(DERIVED_ID_HEADER) if DERIVED_ID_HEADER else None

    # Use only first occurrence per key (same as mirror logic)
    seen_keys = set()
    parts = [f"H={'|'.join(ordered + ([DERIVED_ID_HEADER] if DERIVED_ID_HEADER else []))}"]
    n = 0

    for row in body_rows:
        prow = pad_row(row, len(current_headers))
        k = prow[key_col_idx_0] if key_col_idx_0 < len(prow) else ""
        if not k:
            continue
        sk = str(k)
        if sk in seen_keys:
            continue
        seen_keys.add(sk)
        n += 1

        vals = []
        for ci in mapped_positions:
            vals.append(prow[ci] if ci < len(prow) else "")
        if DERIVED_ID_HEADER:
            vals.append(prow[derived_pos] if (derived_pos is not None and derived_pos < len(prow)) else "")
        parts.append(sk + "|" + "|".join(vals))

    # Match glide hash prefix that includes N=
    parts.insert(0, f"N={n}")
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()

# =========
# Minimal logging
# =========
RUN_SUMMARY_COLUMNS = [
    "timestamp_ist","run_id","script_version","job","sheet_name",
    "rows_in_glide","rows_in_sheet_before","rows_in_sheet_after",
    "inserted","updated_cells","deleted","duplicates_deleted",
    "glide_hash_prev","glide_hash_new","result","error_message"
]
DETAIL_COLUMNS = [
    "timestamp_ist","run_id","action","row_id","sheet_row_num",
    "column_name","old_value","new_value"
]

def snapshot_row(values: List[str], current_headers: List[str], selected_headers: List[str]) -> str:
    """
    Compact JSON snapshot of mapped columns for logging.
    Includes only selected_headers (mapped + ID).
    """
    idx = {h: i for i, h in enumerate(current_headers)}
    snap: Dict[str, str] = {}
    for h in selected_headers:
        i = idx.get(h)
        if i is None:
            continue
        snap[h] = values[i] if i < len(values) else ""
    return json.dumps(snap, ensure_ascii=False, separators=(",", ":"))

def ensure_log_headers(svc):
    if not LOG_ENABLED:
        return
    ensure_sheet_exists(svc, LOG_SHEET_ID, RUN_LOG_TAB)
    if get_header(svc, LOG_SHEET_ID, RUN_LOG_TAB) != RUN_SUMMARY_COLUMNS:
        set_header_row_values(svc, LOG_SHEET_ID, RUN_LOG_TAB, RUN_SUMMARY_COLUMNS)

    if LOG_DETAILS:
        ensure_sheet_exists(svc, LOG_SHEET_ID, DETAILS_LOG_TAB)
        if get_header(svc, LOG_SHEET_ID, DETAILS_LOG_TAB) != DETAIL_COLUMNS:
            set_header_row_values(svc, LOG_SHEET_ID, DETAILS_LOG_TAB, DETAIL_COLUMNS)

def log_run_summary(svc, row: List[str]):
    if LOG_ENABLED:
        append_rows(svc, LOG_SHEET_ID, RUN_LOG_TAB, [row])

def log_details(svc, rows: List[List[str]]):
    if LOG_ENABLED and LOG_DETAILS and rows:
        append_rows(svc, LOG_SHEET_ID, DETAILS_LOG_TAB, rows)

# =========
# Mirror: same Update/Add/Delete logic as your “working” script
# =========
def mirror(force: bool = False, dry: bool = False, inspect: bool = False):
    if inspect:
        rows = glide_fetch_all()
        cols = sorted({k for r in rows for k in r.keys()}) if rows else []
        print("Available columns:", cols)
        if rows:
            print("Sample row:", json.dumps(rows[0], indent=2, ensure_ascii=False))
        return {"inspected": True, "rows": len(rows)}

    svc = sheets_service()
    ensure_sheet_exists(svc, SHEET_ID, SHEET_NAME)
    ensure_log_headers(svc)

    g_rows = glide_fetch_all()
    selected_headers, g2s = build_selected_headers()
    current_headers = ensure_headers_union_preserve_extras(svc, selected_headers)
    cur_index = {h: i for i, h in enumerate(current_headers)}

    # Key column header in sheet
    key_header = SELECT_COLUMNS.get(UNIQUE_KEY, "") or selected_headers[0]
    if key_header not in current_headers:
        key_header = selected_headers[0]
    key_col_idx_0 = cur_index[key_header]

    # Hashes
    meta_key = f"hash:proj:{GLIDE_TABLE_NAME}:{SHEET_NAME}"
    meta = get_meta_map(svc)
    prev_glide_hash = meta.get(meta_key, "")
    triggered_projects: List[str] = []

    def _meta_trigger_key(legacy_id: str) -> str:
        return f"mfg_triggered:{legacy_id}"
    new_glide_hash = compute_hash_selected_from_rows(g_rows, selected_headers, g2s) if g_rows else "EMPTY"

    run_id = uuid.uuid4().hex[:8]
    ts = now_ist_iso()

    body_before = read_body(svc, SHEET_ID, SHEET_NAME, len(current_headers))
    rows_in_sheet_before = len(body_before)
    rows_in_glide = len(g_rows)

    # If sheet drifted, do NOT skip just because glide hash matches meta
    sheet_hash_now = compute_hash_selected_from_sheet(body_before, current_headers, selected_headers, key_col_idx_0)

    # Glide empty => wipe sheet (same behavior as older)
    if not g_rows:
        if dry:
            return {"dry": True, "deleted": rows_in_sheet_before, "inserted": 0, "updated_cells": 0}

        if rows_in_sheet_before:
            delete_rows_by_numbers(svc, SHEET_ID, SHEET_NAME, list(range(2, 2 + rows_in_sheet_before)))

        log_run_summary(svc, [
            ts, run_id, SCRIPT_VERSION, "project_glide_to_sheet", SHEET_NAME,
            str(rows_in_glide), str(rows_in_sheet_before), "0",
            "0", "0", str(rows_in_sheet_before), "0",
            prev_glide_hash, new_glide_hash, "ok", ""
        ])
        meta[meta_key] = new_glide_hash
        set_meta_map(svc, meta)
        return {"ok": True, "deleted": rows_in_sheet_before, "inserted": 0, "updated_cells": 0}

    # SKIP only if glide unchanged AND sheet already matches glide snapshot
    if not force and prev_glide_hash == new_glide_hash and sheet_hash_now == new_glide_hash:
        log_run_summary(svc, [
            ts, run_id, SCRIPT_VERSION, "project_glide_to_sheet", SHEET_NAME,
            str(rows_in_glide), str(rows_in_sheet_before), str(rows_in_sheet_before),
            "0", "0", "0", "0",
            prev_glide_hash, new_glide_hash, "skipped", ""
        ])
        return {"skipped": True, "reason": "hash-match (glide+sheet)"}

    s2g = {v: k for k, v in g2s.items()}

    # Build sheet map + detect duplicates (same as older script)
    sheet_map: Dict[str, List[str]] = {}
    rownum_by_key: Dict[str, int] = {}
    duplicate_rows_to_delete: List[int] = []

    rn = 1
    for row in body_before:
        rn += 1
        prow = pad_row(row, len(current_headers))
        k = prow[key_col_idx_0] if key_col_idx_0 < len(prow) else ""
        if not k:
            continue
        sk = str(k)
        if sk in sheet_map:
            duplicate_rows_to_delete.append(rn)
        else:
            sheet_map[sk] = [str(x) for x in prow]
            rownum_by_key[sk] = rn

    # Derived ID uses sheet headers (recommended explicit env for stability)
    proj_h, part_h = _pick_derived_fields(selected_headers)

    def derived_id(obj: Dict) -> str:
        if not DERIVED_ID_HEADER:
            return ""
        proj = norm(obj.get(s2g.get(proj_h, ""), "")) if proj_h else ""
        part = norm(obj.get(s2g.get(part_h, ""), "")) if part_h else ""
        return f"{proj} - {part}" if (proj or part) else ""

    def row_into_width(obj: Dict) -> List[str]:
        vals = [""] * len(current_headers)
        for sh in selected_headers:
            if sh == DERIVED_ID_HEADER:
                if sh in cur_index:
                    vals[cur_index[sh]] = derived_id(obj)
                continue
            if sh in cur_index:
                gk = s2g.get(sh, None)
                v = norm(obj.get(gk, "")) if gk else ""
                if sh in DATE_MM_DD_HEADERS and v:
                    v = to_mmddyyyy(v)
                vals[cur_index[sh]] = v
        return vals

    # Build glide map + stable order (first occurrence order, last-write-wins data)
    g_map: Dict[str, List[str]] = {}
    g_order: List[str] = []
    for obj in g_rows:
        k = obj.get(UNIQUE_KEY, "")
        if not k:
            continue
        sk = str(k)
        if sk not in g_map:
            g_order.append(sk)
        g_map[sk] = row_into_width(obj)

    mapped_positions = [cur_index[h] for h in selected_headers if h in cur_index]

    updated_cells: List[Tuple[int, int, str]] = []
    rows_to_append: List[List[str]] = []
    rows_to_delete: List[int] = []
    detail_logs: List[List[str]] = []

    # 1) delete duplicates
    for dup_rn in duplicate_rows_to_delete:
        rows_to_delete.append(dup_rn)
        if LOG_DETAILS:
            detail_logs.append([ts, run_id, "delete_duplicate", "duplicate", str(dup_rn), "(duplicate row)", "", ""])

    # 2) delete keys missing in glide + update changed cells
    seen = set()
    for key, sheet_vals in sheet_map.items():
        new_vals = g_map.get(key)
        if not new_vals:
            rows_to_delete.append(rownum_by_key[key])
            if LOG_DETAILS:
                old_json = snapshot_row(sheet_vals, current_headers, selected_headers)
                detail_logs.append([ts, run_id, "delete", key, str(rownum_by_key[key]), "(entire row)", old_json, ""])
            continue

        seen.add(key)
        for ci in mapped_positions:
            oldv = sheet_vals[ci] if ci < len(sheet_vals) else ""
            newv = new_vals[ci]
            if oldv != newv:
                updated_cells.append((rownum_by_key[key], ci + 1, newv))

                # Trigger PROJECT_UPDATED only when status flips to mfg
                if status_idx is not None and id_idx is not None and ci == status_idx:
                    # legacy_id read from the CURRENT sheet row (same row being updated)
                    legacy_id = sheet_vals[id_idx] if id_idx < len(sheet_vals) else ""
                    legacy_id = str(legacy_id or "").strip()

                    if legacy_id and str(newv or "").strip().lower() == PROJ_MFG_STATUS_VALUE:
                        mk = _meta_trigger_key(legacy_id)
                        if not meta.get(mk):  # one-time
                            triggered_projects.append(legacy_id)
                            meta[mk] = now_ist_iso()
                if LOG_DETAILS:
                    detail_logs.append([ts, run_id, "update", key, str(rownum_by_key[key]), current_headers[ci], oldv, newv])

    # 3) insert missing keys from glide
    current_row_count = len(body_before)
    for key in g_order:
        if key in seen:
            continue
        rows_to_append.append(g_map[key])
        # If a new project row is inserted and it's already in mfg, trigger once.
        if status_idx is not None and id_idx is not None:
            new_row_vals = g_map[key]
            legacy_id = new_row_vals[id_idx] if id_idx < len(new_row_vals) else ""
            legacy_id = str(legacy_id or "").strip()
            status_val = new_row_vals[status_idx] if status_idx < len(new_row_vals) else ""
            if legacy_id and str(status_val or "").strip().lower() == PROJ_MFG_STATUS_VALUE:
                mk = _meta_trigger_key(legacy_id)
                if not meta.get(mk):
                    triggered_projects.append(legacy_id)
                    meta[mk] = now_ist_iso()
        predicted_rownum = 2 + current_row_count + len(rows_to_append)
        if LOG_DETAILS:
            new_json = snapshot_row(g_map[key], current_headers, selected_headers)
            detail_logs.append([ts, run_id, "insert", key, str(predicted_rownum), "(row)", "", new_json])

    if dry:
        return {
            "dry": True,
            "inserted": len(rows_to_append),
            "updated_cells": len(updated_cells),
            "deleted": len(rows_to_delete),
            "duplicates_found": len(duplicate_rows_to_delete),
            "glide_hash_prev": prev_glide_hash,
            "glide_hash_new": new_glide_hash,
            "sheet_hash_now": sheet_hash_now,
        }

    # Apply writes
    if WRITE_MODE == "delta":
        batch_update_cells(svc, SHEET_ID, SHEET_NAME, updated_cells)
        delete_rows_by_numbers(svc, SHEET_ID, SHEET_NAME, rows_to_delete)
        append_rows(svc, SHEET_ID, SHEET_NAME, rows_to_append)
    elif WRITE_MODE == "full":
        end = col_letter(len(current_headers))
        svc.spreadsheets().values().clear(spreadsheetId=SHEET_ID, range=f"'{SHEET_NAME}'!A:ZZZ").execute()
        set_header_row_values(svc, SHEET_ID, SHEET_NAME, current_headers)

        final_rows = [g_map[k] for k in g_order if k in g_map]
        CHUNK = 5000
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
    else:
        raise SystemExit("PROJ_WRITE_MODE must be 'delta' or 'full'.")

    # Logs
    log_details(svc, detail_logs)

    inserted = len(rows_to_append)
    updated_cells_count = len(updated_cells)
    deleted = len(rows_to_delete)
    duplicates_deleted = len(duplicate_rows_to_delete)
    rows_in_sheet_after = rows_in_sheet_before - deleted + inserted

    log_run_summary(svc, [
        ts, run_id, SCRIPT_VERSION, "project_glide_to_sheet", SHEET_NAME,
        str(rows_in_glide), str(rows_in_sheet_before), str(rows_in_sheet_after),
        str(inserted), str(updated_cells_count), str(deleted), str(duplicates_deleted),
        prev_glide_hash, new_glide_hash, "ok", ""
    ])

    # Persist glide hash + any "mfg_triggered:<legacy_id>" flags
    meta[meta_key] = new_glide_hash
    set_meta_map(svc, meta)

    # Emit PROJECT_UPDATED one per legacy_id (one-time due to meta flags above)
    for legacy_id in sorted(set([x for x in triggered_projects if str(x).strip()])):
        emit_zai_event("PROJECT_UPDATED", {"legacy_id": legacy_id})

    return {
        "ok": True,
        "inserted": inserted,
        "updated_cells": updated_cells_count,
        "deleted": deleted,
        "duplicates_deleted": duplicates_deleted,
        "glide_hash_prev": prev_glide_hash,
        "glide_hash_new": new_glide_hash,
        "sheet_hash_before": sheet_hash_now,
    }

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Project: Glide → Sheet strict mirror (delta/full) with sheet-drift detection")
    p.add_argument("--force", action="store_true", help="Ignore hash short-circuit")
    p.add_argument("--dry", action="store_true", help="Show planned changes only; no writes")
    p.add_argument("--inspect", action="store_true", help="Print available Glide columns; no writes")
    args = p.parse_args()

    res = mirror(force=args.force, dry=args.dry, inspect=args.inspect)
    print(json.dumps(res, indent=2, ensure_ascii=False))
