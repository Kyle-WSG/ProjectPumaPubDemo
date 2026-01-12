# Project Puma - Daily Diary (Wireline Activity Log)

Project Puma is a Streamlit app for capturing one shift per user per day (single vehicle + single site) with multiple activities. It stores data in SQLite by default and switches to Snowflake automatically when a Snowpark session is active.

This repo is a demo build (VERSION Alpha_2025_7) with a styled UI, activity validation, and a mock well report workflow (in development).

## Core workflow
- Login by selecting a user from `config/users.json` (no auth).
- Create or edit a shift for the selected date (client, site, job number, vehicle, start/end, notes).
- Add activities inside the shift window with codes, tools, hole IDs, and notes.
- Review the timeline and coverage, then edit or delete activities as needed.

## UI behavior details
- Clients are hard-coded in `app.py` (CLIENTS list).
- Site options come from unique vehicle locations in `config/vehicles_catalog.json` plus "Other (manual)".
- Vehicle selection uses the catalog; "Other / not listed" lets you enter a one-off vehicle snapshot stored only on the shift.
- Shift end time is captured in the UI but stored as `shift_hours`; overnight shifts are supported.
- Activity times use 15-minute increments inside the shift window and default to the first free 30-minute slot.
- Overlapping activities are blocked with a conflict warning.
- LOG activities allow Hole ID (blank -> auto-generated UUID) and tools; tools are stored as a comma-separated string.
- Coverage is calculated only for time inside the shift window; the timeline uses Plotly and highlights the activity being edited.
- A version badge is rendered at the bottom from `VERSION` in `app.py`.

## Well report (demo)
- Only available for LOG activities via the edit form.
- Stored in Streamlit session state only (not persisted to SQLite/Snowflake).
- Sections include summary fields, hangup depths per tool, calibration table, DGPS, and comments.
- Excel export is generated with openpyxl.

## Backends and storage
- Backend selection: `storage.backend()` returns "snowflake" when `get_active_session()` exists, otherwise "sqlite".
- SQLite file: `data/project_puma.db` (WAL enabled).
- On app boot, vehicles from `config/vehicles_catalog.json` are upserted into the DB (cached via `st.cache_resource`; restart or clear cache to reload).

## Data model (SQLite, current)
vehicles
- barcode (PK), name, description, model, category, location

holes
- hole_id (PK), created_at, updated_at

shifts (one per user per date)
- id (PK), shift_date, username, client, site, site_other, job_number
- vehicle_barcode, vehicle_name, vehicle_description, vehicle_model, vehicle_category
- vehicle_location_expected, vehicle_location_actual, vehicle_location_mismatch
- shift_start, shift_hours, shift_notes, created_at, updated_at

activities
- id (PK), shift_id (FK), start_ts, end_ts, code, label, notes, tool, hole_id (FK), created_at, updated_at

Relationships:
- shifts 1->many activities (CASCADE delete)
- holes 1->many activities (SET NULL on delete)
- vehicles are used for dropdowns; shift stores a snapshot of vehicle fields

## Snowflake schema (current)
- PUMA_VEHICLES(BARCODE, NAME, DESCRIPTION, MODEL, CATEGORY, LOCATION, UPDATED_AT)
- PUMA_SHIFTS(SHIFT_DATE, USERNAME, CLIENT, SITE, SITE_OTHER, JOB_NUMBER, VEHICLE_*, VEHICLE_LOCATION_*, SHIFT_START, SHIFT_HOURS, SHIFT_NOTES, CREATED_AT, UPDATED_AT)
- PUMA_HOLES(HOLE_ID, CREATED_AT, UPDATED_AT)
- PUMA_ACTIVITIES(ID, SHIFT_DATE, USERNAME, START_TS, END_TS, CODE, LABEL, NOTES, TOOL, HOLE_ID, CREATED_AT, UPDATED_AT)

Notes:
- Snowflake activities are keyed by SHIFT_DATE + USERNAME instead of shift_id.
- HOLE_ID is merged into PUMA_HOLES for LOG activities.

## Migrations and legacy compatibility
- Shifts are rebuilt if columns are missing or extra; legacy fields (active_user, site_name, vehicle) are mapped into the canonical schema.
- Duplicate shifts for the same date/user are deduped by keeping the most recently updated row and reattaching activities.
- Activities are rebuilt if legacy columns exist or the hole FK is missing; start/end are backfilled from legacy columns.
- LOG activities always get a hole_id (UUID if missing) and holes are upserted on add/update.

## Configuration
- `config/users.json`: `{"users": ["Kyle", "..."]}` - user dropdown (no auth).
- `config/catalog.json`: activity_codes (code + label) and tools list.
- `config/vehicles_catalog.json`: vehicles list with barcode, name, description, model, category, location.
- `config/vehicles.json` and `config/sites_by_client.json` are legacy files and are not used by the current app.

## Running locally
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

For a Snowflake environment, use `environment.yml` (includes `snowflake-snowpark-python`).

## Entrypoints
- `app.py`: main Streamlit app.
- `streamlit_app.py`: thin wrapper used by Streamlit Cloud.

## Repo map
- `app.py`: UI, validation, timeline rendering, well report demo.
- `storage.py`: storage backend selection, SQLite schema, migrations, CRUD.
- `config/*.json`: users, activity catalog, vehicle catalog.
- `data/`: SQLite database (created at runtime).
- `backups/`: backups created by `fix_puma.sh` (if used).
- `fix_puma.sh`: legacy patch-and-backup script (targets a different path).

## Planned schema (design notes - not implemented)
This section documents the target schema for RAW -> MASTER separation, a well report view, and deliverables tracking. It is not used by the current code paths.

### Key concepts
- PK: Primary Key (unique row id)
- FK: Foreign Key (pointer from child to parent)
- UQ: Unique constraint (prevents duplicates)
- CASCADE: delete parent deletes children
- SET NULL: delete parent keeps child but unlinks the FK

### RAW vs MASTER
RAW tables capture inbound data as-is. MASTER tables are curated for reporting, preserving auditability.

### Tables (target)
1) raw_drilled_holes (RAW input)
- Source: daily client email/attachment parse.
- Columns: raw_id (PK), client, received_at, source_email_id, client_hole_ref, drilled_date,
  drilled_depth, payload_json (full parsed row).

2) holes (MASTER)
- Curated holes derived from raw_drilled_holes.
- Columns: hole_id (PK), client, client_hole_ref (UQ), drilled_date, drilled_depth,
  created_at, updated_at.

3) raw_well_reports (RAW input)
- All user-entered well report fields (wide columns or raw_json).
- Columns: raw_wr_id (PK), hole_id (FK, nullable), user_id (FK), source, raw_json,
  created_at, updated_at.

4) vw_well_reports (VIEW)
- Projection from raw_well_reports (and joins) exposing only required clean fields.

5) users (MASTER)
- Operator identities and audit attribution.
- Columns: user_id (PK), username (UQ), display_name, role, is_active, created_at, updated_at.

6) vehicles (MASTER)
- Vehicle catalog for dropdowns and references.
- Columns: barcode (PK), name, description, model, category, location, created_at, updated_at.

7) shifts (WORKFLOW container)
- A shift is a block of work for a user (can be multiple per day).
- Columns: shift_id (PK), user_id (FK), shift_date, shift_start, shift_hours, client, site,
  site_other, job_number, vehicle_barcode (FK), shift_notes, created_at, updated_at.
- UQ (user_id, shift_date, shift_start) allows day/night/callout shifts.

8) activities (WORKFLOW time blocks)
- Generic timeline entries for a shift.
- Columns: activity_id (PK), shift_id (FK, CASCADE), hole_id (FK, SET NULL), start_ts, end_ts,
  code, label, notes, tool, created_at, updated_at.

9) logging_activities (LOG subtype, 1:1)
- LOG-only extension table (exists only if activities.code == "LOG").
- Example fields: depth_from, depth_to, tool_string, position, status, created_at, updated_at.

10) logging_files
- Uploaded tool logs for a LOG activity.
- Columns: log_file_id (PK), activity_id (FK, CASCADE), file_role, storage_uri, checksum,
  original_name, file_size, uploaded_by (FK), uploaded_at.

11) las_files
- Generated LAS outputs for a LOG activity (versioned).
- Columns: las_file_id (PK), activity_id (FK, CASCADE), storage_uri, version, created_at.

12) deliverables
- A deliverable ZIP container for a hole.
- Columns: deliverable_id (PK), hole_id (FK), zip_uri, status, created_at.

13) deliverable_files
- Full inventory of files inside a deliverable ZIP.
- Columns: deliverable_file_id (PK), deliverable_id (FK, CASCADE), file_kind, file_uri, created_at.

### Relationships summary (target)
- users 1 -> many shifts
- vehicles 1 -> many shifts
- shifts 1 -> many activities (CASCADE)
- holes 1 -> many activities (LOG only sets hole_id; others NULL)
- activities 0/1 -> logging_activities (LOG subtype)
- logging_activities 1 -> many logging_files
- logging_activities 1 -> many las_files
- holes 1 -> many deliverables
- deliverables 1 -> many deliverable_files
- raw_drilled_holes -> holes (derived/upsert)
- raw_well_reports -> vw_well_reports (view/projection)

### Adoption status (current code vs target)
Implemented today (SQLite):
- vehicles, holes, shifts, activities tables exist (see `storage.py`).
- shifts is unique per (shift_date, username).
- activities stores LOG hole_id and links to holes (FK).

Not implemented yet (planned):
- raw_drilled_holes, raw_well_reports, vw_well_reports.
- users table (app uses `config/users.json`).
- logging_activities, logging_files, las_files.
- deliverables, deliverable_files.
- shift/user IDs (currently uses username string, not user_id).

### Notes for SQLite vs Snowflake
- SQLite: sync/rebuild step can upsert holes from raw_drilled_holes.
- Snowflake: view/dynamic table or task+merge can keep holes current from RAW.
