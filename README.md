# Glide ↔ Google Sheets Mirror (Render Cron)

This repo runs 4 Python jobs on a schedule to sync data between Google Sheets and Glide (legacy API). It is designed to be run as a Render **Cron Job** using `python run_all.py`.

---

## What it does

### Job 01 — Dashboard Updates (append-only)
- Syncs dashboard updates in an append-only pattern (dedupe key-based).

### Job 02 — Project (Glide → Sheet)
- Pulls Project table from Glide into the `Project` sheet (delta mode supported).

### Job 03 — CCP (two-way)
- Two-way sync between the `CCP` sheet and Glide CCP table.
- Uses timestamps (`Updated At` / `Updated By`) to decide direction.

### Job 04 — Suppliers (Sheet → Glide)
- Pushes Suppliers from `Suppliers capmap` sheet to Glide Suppliers table.
- Uses the Sheet `ID` column as the unique business key in Glide.
- **Pagination is implemented** to prevent re-adding rows on cron reruns.

---

## Repo layout

```
.
├── run_all.py
└── jobs/
    ├── 01_dashboard_updates_append_only.py
    ├── 02_project_glide_to_sheet.py
    ├── 03_ccp_two_way.py
    └── 04_suppliers_sheet_to_glide.py
```

---

## Requirements

- Python 3.10+ recommended
- A Google Service Account with access to:
  - main Sheet (`GOOGLE_SHEET_ID`)
  - log Sheet (`LOG_SHEET_ID`)
- Glide Legacy API access:
  - `GLIDE_TOKEN`
  - `GLIDE_APP_ID`

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Environment variables

### Render setup rule

In Render, add each key as a separate env var (do not paste a .env blob into one field).

> **Important**  
> For JSON env vars like `*_GLIDE_COLUMNS_JSON` / `PROJ_SELECT_COLUMNS`:
> - Use raw JSON (no surrounding single quotes).
> - Example ✅:
>   ```json
>   {"ID":"O0rtV","Company_Name":"Name"}
>   ```
> - Example ❌:
>   ```json
>   '{"ID":"O0rtV","Company_Name":"Name"}'
>   ```

### Minimum required env vars

#### Global
- `GOOGLE_SHEET_ID`
- `GOOGLE_CREDENTIALS_JSON` (full service account JSON)
- `GLIDE_BASE_URL` (default: `https://api.glideapp.io`)
- `GLIDE_TOKEN`
- `GLIDE_APP_ID`

#### Logging
- `LOG_ENABLED` (`true`/`false`)
- `LOG_SHEET_ID`
- `SCRIPT_VERSION`
- `META_SHEET` (default `_meta`)

#### Job 01
- `DASH_SHEET_TAB`
- `DASH_GLIDE_TABLE_NAME`
- `DASH_SHEET_SYNCKEY_HEADER`
- `DASH_GLIDE_SYNCKEY_COL`
- `DASH_GLIDE_COLUMNS_JSON`
- `DASH_LOG_DETAILS_TAB`
- `DASH_LOG_SUMMARY_TAB`

#### Job 02
- `PROJ_SHEET_NAME`
- `PROJ_WRITE_MODE`
- `PROJ_UNIQUE_KEY`
- `PROJ_DERIVED_ID_HEADER`
- `PROJ_SELECT_COLUMNS`
- `PROJ_GLIDE_TABLE_NAME`
- `PROJ_GLIDE_MODE`
- `PROJ_LOG_DETAILS_TAB`
- `PROJ_LOG_SUMMARY_TAB`

#### Job 03
- `CCP_SHEET_TAB`
- `CCP_GLIDE_TABLE_NAME`
- `CCP_SHEET_KEY_HEADER`
- `CCP_SHEET_GLIDE_ROWID_HEADER`
- `CCP_SHEET_UPDATED_AT_HEADER`
- `CCP_SHEET_UPDATED_BY_HEADER`
- `CCP_GLIDE_UPDATED_AT_COL`
- `CCP_GLIDE_UPDATED_BY_COL`
- `CCP_CONFLICT_WINNER`
- `CCP_GLIDE_COLUMNS_JSON`
- `CCP_LOG_DETAILS_TAB`
- `CCP_LOG_SUMMARY_TAB`

#### Job 04
- `SUP_SHEET_TAB`
- `SUP_GLIDE_TABLE_NAME`
- `SUP_SHEET_KEY_HEADER`
- `SUP_SHEET_GLIDE_ROWID_HEADER`
- `SUP_GLIDE_COLUMNS_JSON`
- `SUP_GLIDE_POINTER_COL` (must be empty)
- `SUP_LOG_DETAILS_TAB`
- `SUP_LOG_SUMMARY_TAB`
- Optional safety:
  - `SUP_SKIP_EMPTY_OVERWRITE` (`true` default)

---

## Run locally

```bash
python run_all.py
```

Run only one job:

```bash
python jobs/04_suppliers_sheet_to_glide.py
```

---

## Deploy on Render (Cron Job)

1. Create **Cron Job** (not Web Service)
2. Select repo + branch
3. **Build command**:
   ```bash
   pip install -r requirements.txt
   ```
4. **Start command**:
   ```bash
   python run_all.py
   ```
5. **Schedule examples** (UTC):
   - Every 2 min: `*/2 * * * *`
   - Every 5 min: `*/5 * * * *`
   - Every 10 min: `*/10 * * * *`
6. Add all env vars in Render → Environment

---

## Operational notes

- Logs are written to `LOG_SHEET_ID` in:
  - details tab: `*_change_details`
  - summary tab: `*_run_summary`
- Suppliers job is idempotent because:
  - it fetches all Glide rows via pagination
  - it matches by normalized ID (`strip().upper()`)

---

## Troubleshooting

### JSON mapping errors (`Invalid ... JSON`)
**Fix**: Remove surrounding single quotes in Render env var values.

### Suppliers keep "re-adding" on cron rerun
**Fix**: Ensure Suppliers script includes Glide pagination and that the Glide key column mapping is correct for `ID`.

### BrokenPipe / grid limit errors in logs
This repo uses auto sheet resize for log appends. If failures persist, reduce log volume or increase batching.