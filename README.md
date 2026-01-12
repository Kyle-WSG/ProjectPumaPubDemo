# Project Puma - Daily Diary (Wireline Activity Log)

Project Puma is a Streamlit app for daily wireline shift logging. Users pick a date, complete shift details, then add activities. The UI shows a timeline and a coverage bar so teams can see how much of the shift has been logged.

This repo is a demo build (VERSION Alpha_2025_7) with a styled UI and a demo well report workflow.

## What the app does (plain English)
- Users can record multiple shifts per day, then switch between them.
- A shift captures who/where/when: client, site, job number, vehicle, shift start/end.
- Activities capture what happened during the shift window: code, times, tools, hole name, notes.
- Logging activities (code LOG) can store attachments and a well report.

## UI flow (how a user moves through the app)
1) Login: choose a user name.
2) Date: use the top bar to move to the correct day.
3) Shift: create a shift (or switch between multiple shifts for that day).
4) Activities: add activities inside the shift time window.
5) Edit activity: opens a dedicated activity page with all details, files, and well report.

Important view states (stored in `st.session_state.view`):
- `dd`: main shift dashboard (timeline, activity list).
- `create_shift`: create another shift for the same date.
- `switch_shift`: select which shift is active.
- `edit_shift`: edit the active shift.
- `add_activity`: add a new activity.
- `edit_activity`: full-page activity editor.

## Key rules and validations
- Multiple shifts per day are allowed. The active shift is stored in session state (`active_shift_id`).
- Shift details must be complete before activities can be added.
- Shift end time is stored as `shift_hours` (hours between start and end; wraps past midnight if needed).
- Activity times are chosen in 15-minute steps within the shift window.
- Activities cannot overlap in time.
- LOG activities always have a hole_id (internal UUID).
- hole_name is user-defined and not unique; duplicates are allowed.
- Creating a LOG activity never auto-dedupes by hole_name; reuse is explicit.
- Activity codes are locked after creation. To change a code, delete and recreate the activity.

## Activity edit page (single page)
When you edit an activity, the app switches to a dedicated page that contains everything:
- Title shows: Logging (if LOG) or the activity label, plus Location, Hole name (LOG), and Tools.
- Time window editor (start/end).
- Tools selector for LOG and CAL only.
- Hole name controls for LOG (new hole by default or explicitly select an existing hole).
- Hole ID is shown only as a read-only advanced detail.
- Selecting an existing hole shows a disambiguated label (name + short ID + created time); renaming affects all activities linked to that hole_id.
- Notes.
- LOG file uploads and status management.
- Well report editor (embedded for LOG only).
- Delete section with explicit warnings and confirmation.

## File attachments (LOG only)
Files are stored in the database and linked to the activity.
Statuses:
- `pending`: uploaded but not yet attached. These become `active` when the user saves the activity.
- `active`: attached to the activity.
- `redundant`: older files the user marked as not current.

Behavior:
- Users can upload multiple files per LOG activity.
- Pending files can be removed before save.
- After saving, files cannot be deleted, only marked redundant.
- Redundant files can be restored to active (toggle).
- Each file has a download button.

## Well report (demo)
- The well report is available only for LOG activities.
- It is a demo editor stored in Streamlit session state (not persisted to the database).
- Users can download the report as Excel.

## Date and time formatting
All visible date/time strings are shown as:
`DD/MM/YYYY at HH:MM`

## Persistence and backends
Backend selection:
- Default: SQLite at `data/project_puma.db`.
- Snowflake: if a Snowpark session is active, all reads/writes use PUMA_* tables.

SQLite:
- Tables are created/updated on boot via `storage.init_storage()`.
- Legacy migrations are applied automatically.
- Orphaned activities (missing shift) are skipped during migration.

Snowflake:
- Uses `SHIFT_ID` to link shifts and activities when multiple shifts exist.
- Activity file bytes are stored in `PUMA_ACTIVITY_FILES`.

## Configuration files
- `config/users.json`: user list for the login dropdown.
- `config/catalog.json`: activity codes and tools list (defaults are used if empty).
- `config/vehicles_catalog.json`: vehicle list; locations are used to build the site dropdown.
- `config/vehicles.json` and `config/sites_by_client.json` are legacy and not used.

## Running locally
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

For a Snowflake environment, use `environment.yml` (includes `snowflake-snowpark-python`).

## File map
- `app.py`: main Streamlit UI, validation, timeline, activity edit page, well report demo.
- `storage.py`: backend selection, SQLite schema/migrations, Snowflake IO, CRUD.
- `streamlit_app.py`: thin wrapper used by Streamlit Cloud.
- `config/*.json`: users, activity catalog, vehicle catalog.
- `data/`: SQLite database file (created at runtime).
- `backups/`: backups created by `fix_puma.sh` (if used).
- `fix_puma.sh`: legacy patch-and-backup script (use with care).

## Holes: name vs ID
- hole_id: internal unique identifier (UUID). Used for relations. Not shown as the primary label.
- hole_name: user-entered display name. Not unique. Two different holes can share the same name.
- Creating a LOG activity defaults to creating a new hole with the provided hole_name.
- Reusing a hole is always explicit (via "Use existing hole").
- Legacy holes may have no hole_name; the UI shows "Unnamed hole" with a short ID as fallback.

## SQLite schema (current)
vehicles
- barcode (PK), name, description, model, category, location

holes
- hole_id (PK), hole_name, created_at, updated_at

shifts
- id (PK), shift_date, username, client, site, site_other, job_number
- vehicle_barcode, vehicle_name, vehicle_description, vehicle_model, vehicle_category
- vehicle_location_expected, vehicle_location_actual, vehicle_location_mismatch
- shift_start, shift_hours, shift_notes, created_at, updated_at

activities
- id (PK), shift_id (FK), start_ts, end_ts, code, label, notes, tool, hole_id (FK), created_at, updated_at

activity_files (LOG attachments)
- id (PK), activity_id (FK), file_name, file_bytes, file_size, checksum, status, uploaded_at, uploaded_by

Relationships:
- shifts -> activities (cascade delete)
- holes -> activities (set null on delete)
- vehicle fields are copied into shifts as a snapshot

## Snowflake tables
- PUMA_VEHICLES(BARCODE, NAME, DESCRIPTION, MODEL, CATEGORY, LOCATION, UPDATED_AT)
- PUMA_SHIFTS(SHIFT_ID, SHIFT_DATE, USERNAME, CLIENT, SITE, SITE_OTHER, JOB_NUMBER, VEHICLE_*, VEHICLE_LOCATION_*, SHIFT_START, SHIFT_HOURS, SHIFT_NOTES, CREATED_AT, UPDATED_AT)
- PUMA_HOLES(HOLE_ID, HOLE_NAME, CREATED_AT, UPDATED_AT)
- PUMA_ACTIVITIES(ID, SHIFT_ID, SHIFT_DATE, USERNAME, START_TS, END_TS, CODE, LABEL, NOTES, TOOL, HOLE_ID, CREATED_AT, UPDATED_AT)
- PUMA_ACTIVITY_FILES(ID, ACTIVITY_ID, SHIFT_ID, FILE_NAME, FILE_BYTES, FILE_SIZE, CHECKSUM, STATUS, UPLOADED_AT, UPLOADED_BY)

Notes:
- Snowflake activities are linked by SHIFT_ID.
- LOG activities upsert into PUMA_HOLES.

## Planned schema (design notes, not implemented)
This is a high-level outline of the target RAW -> MASTER model and well report deliverables.

- raw_drilled_holes, raw_well_reports (raw input tables)
- vw_well_reports (view)
- users, vehicles (master data)
- shifts, activities (workflow data)
- logging_activities, logging_files, las_files (LOG extensions)
- deliverables, deliverable_files (exports)
