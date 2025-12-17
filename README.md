# Project Puma — Daily Diary (Wireline Activity Log)

**Forked for a December demo**: this branch carries the current UX with theme toggle, editing flows, and shift/activity validation.

Project Puma is a Streamlit app for capturing one shift per user per day (single vehicle + single site) with multiple activities. It stores data in SQLite by default and can switch to Snowflake automatically if a Snowpark session is active.

## What it does
- Login from `config/users.json` (simple dropdown, no auth).
- Per day, per user: create or edit a shift (client, site, job number, vehicle snapshot, start/end times, optional notes).
- Add activities with start/end, code/label, tool (for LOG/CAL), notes; conflict checks prevent overlaps and defaults pick the first free slot in the shift window.
- Timeline shows the full shift window plus activities, with coverage bar. Edit mode highlights the active activity.
- Activities can be edited or deleted inline; cards show time range, duration, code colors, tool/notes hints.

## Data model
SQLite DB at `data/project_puma.db` (Snowflake when a Snowpark session is available).

- `vehicles`: `barcode` (PK), `name`, `description`, `model`, `category`, `location`
- `shifts` (one per user per date): `id`, `shift_date`, `username`, `client`, `site`, `site_other`, `job_number`, `vehicle_*` snapshot, `shift_start`, `shift_hours`, `shift_notes`, `created_at`, `updated_at`
- `activities`: `id`, `shift_id` (FK), `start_ts`, `end_ts`, `code`, `label`, `tool`, `notes`, `created_at`, `updated_at`

Legacy data is migrated on startup; duplicates are deduped by keeping the latest and reattaching activities.

## Config
- `config/users.json` — user list for login.
- `config/catalog.json` — activity codes + tools for the add/edit activity forms.
- `config/vehicles_catalog.json` — vehicle master data (barcode, name, description, model, category, location).

## Running locally
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Code map
- `app.py`: Streamlit UI; handles login, theming, shift CRUD, activity add/edit/delete, timeline/coverage, conflict checks, and layout.
- `storage.py`: Data layer; initializes/migrates SQLite (or Snowflake), enforces one shift per user/day, snapshots vehicle/location data, and provides CRUD for shifts/activities.
- `config/*.json`: Inputs for users, activity codes/tools, vehicle catalog.
