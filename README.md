# Glide ↔ Google Sheets Sync Jobs (isolated per table)

Repo layout:
- jobs/01_dashboard_updates_append_only.py  (append-only both ways)
- jobs/02_project_glide_to_sheet.py         (Glide -> Sheet mirror)
- jobs/03_ccp_two_way.py                    (two-way sync with conflict policy)
- jobs/04_suppliers_sheet_to_glide.py       (Sheet -> Glide)

## Setup
1) Create `.env` (copy from `.env.example`)
2) Install deps:
   pip install -r requirements.txt

## Run
Run everything:
   python run_all.py

Or run a single job:
   python jobs/03_ccp_two_way.py

## Mapping-driven
To add/remove synced columns, edit JSON mappings in `.env`:
- DASH_GLIDE_COLUMNS_JSON (SheetHeader -> GlideColumnId)
- PROJ_SELECT_COLUMNS     (GlideKey -> SheetHeader)
- CCP_GLIDE_COLUMNS_JSON  (SheetHeader -> GlideColumnId)
- SUP_GLIDE_COLUMNS_JSON  (SheetHeader -> GlideColumnId)

No code changes needed for additional columns, as long as the headers exist in the Sheet and the Glide column IDs are correct.