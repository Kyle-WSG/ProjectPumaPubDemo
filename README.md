# Project Puma — Daily Diary (Wireline Activity Log)

Modern Streamlit UI for logging **one shift per user per day** (single vehicle + single site) with **multiple activities** per shift. Includes coverage bar that fills as you add activities.

## How it works
- Login with a user from `config/users.json` (temporary, no auth).
- Pick a shift date (no day/night split; exactly one shift per user per day).
- Create/edit the shift: client, site, job number (required), vehicle (from catalog or manual), optional notes. Vehicle location is auto-filled from the catalog; if edited, the shift is flagged with `vehicle_location_mismatch` and both expected/actual locations are stored.
- Add activities with start/end times, code, label, optional tool and notes. Coverage bar shows how much of the 12-hour window is filled by activities.
- List and delete activities inline.

## Data model
SQLite at `data/project_puma.db` (Snowflake supported if a Snowpark session is active).

### vehicles
- `barcode` (PK), `name`, `description`, `model`, `category`, `location`

### shifts` (one per user per date)
- `id` (PK), `shift_date` (ISO date), `username`
- `client`, `site`, `site_other`
- `job_number`
- Vehicle snapshot: `vehicle_barcode`, `vehicle_name`, `vehicle_description`, `vehicle_model`, `vehicle_category`
- Location tracking: `vehicle_location_expected`, `vehicle_location_actual`, `vehicle_location_mismatch` (0/1)
- `shift_start` (HH:MM), `shift_hours` (default 12), `shift_notes`
- `created_at`, `updated_at`
- Unique per (`shift_date`, `username`); legacy duplicates are deduped by keeping the latest and reattaching activities.

### activities
- `id` (PK), `shift_id` (FK → shifts.id)
- `start_ts`, `end_ts` (ISO datetime strings)
- `code`, `label`, `tool`, `notes`
- `created_at`, `updated_at`

Relationships: **shifts (1) → (many) activities**; vehicle data is snapshotted onto the shift to keep historical context even if the catalog changes.

## Config
- `config/users.json` — `{"users":[...]} ` dropdown for login.
- `config/catalog.json` — activity codes + tools shown in the add-activity form.
- `config/vehicles_catalog.json` — generated from the supplied CSV (barcode, name, description, model, category, location). Used to auto-fill vehicle/location and to populate the vehicle dropdown.

## Running locally
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Code map
- `app.py`: Streamlit UI. Handles login, date navigation, shift form, activity form, coverage bar, and activity list.
- `storage.py`: Data layer. Initializes/migrates SQLite (or Snowflake), enforces one shift per user per day, dedupes legacy rows, snapshots vehicle/location data, and provides CRUD for shifts/activities.
- `config/*.json`: Inputs for users, activity codes/tools, vehicle catalog.
