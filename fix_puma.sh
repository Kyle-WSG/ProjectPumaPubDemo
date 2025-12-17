cd "/home/panza/Documents/Codework/Data Processing/ProjectPuma" && \
pkill -f "streamlit run" 2>/dev/null || true && \
mkdir -p backups && \
STAMP="$(date +%Y%m%d_%H%M%S)" && \
cp -f app.py "backups/app.py.$STAMP.bak" 2>/dev/null || true && \
cp -f storage.py "backups/storage.py.$STAMP.bak" 2>/dev/null || true && \
[ -f data/project_puma.db ] && cp -f data/project_puma.db "backups/project_puma.db.$STAMP.bak" || true && \
python3 - <<'PY'
from __future__ import annotations
from pathlib import Path
import textwrap

def write(path: str, content: str):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")

storage_py = r"""
import os, json, sqlite3, time
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

DB_PATH = os.path.join("data", "project_puma.db")

CLIENTS = ["RTIO", "RTC", "FMG", "FMGX", "Roy Hill", "Other"]
LOGGING_CODES = {"LOG", "CAL"}

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")

def try_get_sf_session():
    try:
        from snowflake.snowpark.context import get_active_session  # type: ignore
        return get_active_session()
    except Exception:
        return None

_SF_SESSION = None

def backend() -> str:
    global _SF_SESSION
    if _SF_SESSION is None:
        _SF_SESSION = try_get_sf_session()
    return "snowflake" if _SF_SESSION is not None else "sqlite"

def _sf():
    global _SF_SESSION
    if _SF_SESSION is None:
        _SF_SESSION = try_get_sf_session()
    if _SF_SESSION is None:
        raise RuntimeError("Snowflake session not available")
    return _SF_SESSION

def _q(v: Optional[str]) -> str:
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"

def _sqlite_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=30000;")
    # WAL helps a lot under Streamlit reruns
    for i in range(30):
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(0.10 * (i + 1))
                continue
            raise
    return conn

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;", (name,)
    ).fetchone()
    return row is not None

def _cols(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()}

def _ensure_col(conn: sqlite3.Connection, table: str, col: str, ddl: str) -> None:
    if col in _cols(conn, table):
        return
    for i in range(30):
        try:
            conn.execute(ddl)
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(0.10 * (i + 1))
                continue
            raise

def _parse_date(s: str) -> date:
    return datetime.fromisoformat(s).date()

def _coerce_ts(datestr: Optional[str], t: Optional[str]) -> Optional[str]:
    if not t:
        return None
    t = str(t).strip()
    if not t:
        return None
    # Already ISO-ish
    if "T" in t:
        try:
            datetime.fromisoformat(t)
            return t[:19]
        except Exception:
            pass
    if not datestr:
        return None
    # HH:MM or HH:MM:SS
    if len(t) == 5:
        t = t + ":00"
    try:
        datetime.fromisoformat(f"{datestr}T{t}")
        return f"{datestr}T{t}"
    except Exception:
        return None

def init_storage() -> None:
    if backend() == "sqlite":
        _init_sqlite()
    else:
        _init_snowflake()
    upsert_reference_data()

def _init_sqlite() -> None:
    conn = _sqlite_conn()

    # Base tables (won't overwrite existing)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS vehicles(
      barcode TEXT PRIMARY KEY,
      name TEXT, description TEXT, model TEXT, category TEXT, location TEXT
    );""")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS shifts(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      shift_date TEXT NOT NULL,
      username TEXT NOT NULL
    );""")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS activities(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      shift_id INTEGER NOT NULL,
      created_at TEXT,
      updated_at TEXT,
      FOREIGN KEY(shift_id) REFERENCES shifts(id) ON DELETE CASCADE
    );""")

    # --- Shift columns (new model) ---
    # Support legacy naming: active_user -> username
    if "active_user" in _cols(conn, "shifts") and "username" not in _cols(conn, "shifts"):
        _ensure_col(conn, "shifts", "username", "ALTER TABLE shifts ADD COLUMN username TEXT;")
        conn.execute("UPDATE shifts SET username=active_user WHERE username IS NULL OR username='';")
    # Make sure shift_date exists (most versions have it; if not, add)
    _ensure_col(conn, "shifts", "shift_date", "ALTER TABLE shifts ADD COLUMN shift_date TEXT NOT NULL DEFAULT '1970-01-01';")

    # Required-by-new-UI
    _ensure_col(conn, "shifts", "client", "ALTER TABLE shifts ADD COLUMN client TEXT NOT NULL DEFAULT 'Other';")
    _ensure_col(conn, "shifts", "site", "ALTER TABLE shifts ADD COLUMN site TEXT NOT NULL DEFAULT 'Other';")
    _ensure_col(conn, "shifts", "site_other", "ALTER TABLE shifts ADD COLUMN site_other TEXT;")
    _ensure_col(conn, "shifts", "job_number", "ALTER TABLE shifts ADD COLUMN job_number TEXT NOT NULL DEFAULT 'UNKNOWN';")

    _ensure_col(conn, "shifts", "vehicle_barcode", "ALTER TABLE shifts ADD COLUMN vehicle_barcode TEXT NOT NULL DEFAULT 'UNSET';")
    _ensure_col(conn, "shifts", "vehicle_name", "ALTER TABLE shifts ADD COLUMN vehicle_name TEXT NOT NULL DEFAULT 'UNSET';")
    _ensure_col(conn, "shifts", "vehicle_category", "ALTER TABLE shifts ADD COLUMN vehicle_category TEXT;")
    _ensure_col(conn, "shifts", "vehicle_location_expected", "ALTER TABLE shifts ADD COLUMN vehicle_location_expected TEXT;")
    _ensure_col(conn, "shifts", "vehicle_location_actual", "ALTER TABLE shifts ADD COLUMN vehicle_location_actual TEXT;")
    _ensure_col(conn, "shifts", "vehicle_location_mismatch", "ALTER TABLE shifts ADD COLUMN vehicle_location_mismatch INTEGER NOT NULL DEFAULT 0;")

    _ensure_col(conn, "shifts", "shift_start", "ALTER TABLE shifts ADD COLUMN shift_start TEXT NOT NULL DEFAULT '06:00';")
    _ensure_col(conn, "shifts", "shift_hours", "ALTER TABLE shifts ADD COLUMN shift_hours REAL NOT NULL DEFAULT 12;")
    _ensure_col(conn, "shifts", "shift_notes", "ALTER TABLE shifts ADD COLUMN shift_notes TEXT;")

    _ensure_col(conn, "shifts", "created_at", "ALTER TABLE shifts ADD COLUMN created_at TEXT;")
    _ensure_col(conn, "shifts", "updated_at", "ALTER TABLE shifts ADD COLUMN updated_at TEXT;")

    # Backfill timestamps if missing
    conn.execute("UPDATE shifts SET created_at=COALESCE(created_at, ?), updated_at=COALESCE(updated_at, ?);", (now_iso(), now_iso()))

    # --- Activities columns (new model) ---
    # Legacy columns can exist (start_time/end_time). We add start_ts/end_ts and backfill safely.
    _ensure_col(conn, "activities", "start_ts", "ALTER TABLE activities ADD COLUMN start_ts TEXT;")
    _ensure_col(conn, "activities", "end_ts", "ALTER TABLE activities ADD COLUMN end_ts TEXT;")
    _ensure_col(conn, "activities", "code", "ALTER TABLE activities ADD COLUMN code TEXT NOT NULL DEFAULT 'OTH';")
    _ensure_col(conn, "activities", "label", "ALTER TABLE activities ADD COLUMN label TEXT NOT NULL DEFAULT 'Other';")
    _ensure_col(conn, "activities", "notes", "ALTER TABLE activities ADD COLUMN notes TEXT;")
    _ensure_col(conn, "activities", "tool", "ALTER TABLE activities ADD COLUMN tool TEXT;")

    # Backfill start_ts/end_ts from legacy fields if present
    a_cols = _cols(conn, "activities")
    start_legacy = next((c for c in ["start_time", "start", "begin_time"] if c in a_cols), None)
    end_legacy = next((c for c in ["end_time", "finish_time", "end", "finish"] if c in a_cols), None)

    if start_legacy or end_legacy:
        # Build SELECT dynamically
        sel = ["a.id", "a.shift_id", "s.shift_date", "a.start_ts", "a.end_ts"]
        if start_legacy: sel.append(f"a.{start_legacy}")
        if end_legacy: sel.append(f"a.{end_legacy}")
        rows = conn.execute(
            f"SELECT {', '.join(sel)} FROM activities a LEFT JOIN shifts s ON s.id=a.shift_id "
            "WHERE a.start_ts IS NULL OR a.start_ts='' OR a.end_ts IS NULL OR a.end_ts='';"
        ).fetchall()

        for r in rows:
            datestr = r["shift_date"]
            st_ts = r["start_ts"]
            en_ts = r["end_ts"]
            st_legacy_val = r[start_legacy] if start_legacy else None
            en_legacy_val = r[end_legacy] if end_legacy else None

            if not st_ts:
                st_ts = _coerce_ts(datestr, st_legacy_val)
            if not en_ts:
                en_ts = _coerce_ts(datestr, en_legacy_val)

            # If still missing, set something non-empty so UI doesn't crash
            if not st_ts:
                st_ts = f"{datestr}T00:00:00" if datestr else "1970-01-01T00:00:00"
            if not en_ts:
                en_ts = st_ts

            conn.execute("UPDATE activities SET start_ts=?, end_ts=? WHERE id=?;", (st_ts, en_ts, int(r["id"])))

    # created/updated timestamps in activities
    conn.execute("UPDATE activities SET created_at=COALESCE(created_at, ?), updated_at=COALESCE(updated_at, ?);", (now_iso(), now_iso()))

    # --- Enforce one shift per user per day (dedupe first) ---
    # If duplicates exist, keep the lowest id, move activities, delete extras.
    dupes = conn.execute(
        "SELECT shift_date, username, MIN(id) keep_id, GROUP_CONCAT(id) ids, COUNT(*) n "
        "FROM shifts GROUP BY shift_date, username HAVING n > 1;"
    ).fetchall()
    for d in dupes:
        keep_id = int(d["keep_id"])
        ids = [int(x) for x in str(d["ids"]).split(",") if x.strip().isdigit()]
        for sid in ids:
            if sid == keep_id:
                continue
            conn.execute("UPDATE activities SET shift_id=? WHERE shift_id=?;", (keep_id, sid))
            conn.execute("DELETE FROM shifts WHERE id=?;", (sid,))

    # Unique index (now safe)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_shifts_user_day ON shifts(username, shift_date);")

    # Activities index: only create if the column exists (it will now, but still safe)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_acts_shift_start_ts ON activities(shift_id, start_ts);")

def _init_snowflake() -> None:
    s = _sf()
    s.sql("""
    CREATE TABLE IF NOT EXISTS PUMA_VEHICLES(
      BARCODE STRING, NAME STRING, DESCRIPTION STRING, MODEL STRING,
      CATEGORY STRING, LOCATION STRING, UPDATED_AT TIMESTAMP_NTZ
    );""").collect()

    s.sql("""
    CREATE TABLE IF NOT EXISTS PUMA_SHIFTS(
      SHIFT_DATE DATE, USERNAME STRING,
      CLIENT STRING, SITE STRING, SITE_OTHER STRING, JOB_NUMBER STRING,
      VEHICLE_BARCODE STRING, VEHICLE_NAME STRING, VEHICLE_CATEGORY STRING,
      VEHICLE_LOCATION_EXPECTED STRING, VEHICLE_LOCATION_ACTUAL STRING, VEHICLE_LOCATION_MISMATCH BOOLEAN,
      SHIFT_START STRING, SHIFT_HOURS FLOAT, SHIFT_NOTES STRING,
      CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ
    );""").collect()

    s.sql("""
    CREATE TABLE IF NOT EXISTS PUMA_ACTIVITIES(
      ID NUMBER AUTOINCREMENT,
      SHIFT_DATE DATE, USERNAME STRING,
      START_TS TIMESTAMP_NTZ, END_TS TIMESTAMP_NTZ,
      CODE STRING, LABEL STRING, NOTES STRING, TOOL STRING,
      CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ
    );""").collect()

def _load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def upsert_reference_data() -> None:
    # vehicles catalog (preferred)
    vehicles = _load_json(os.path.join("config", "vehicles_catalog.json"), {}).get("vehicles", [])
    if vehicles:
        upsert_vehicles(vehicles)
        return
    # fallback old list form (vehicles.json)
    vlist = _load_json(os.path.join("config", "vehicles.json"), {}).get("vehicles", [])
    if isinstance(vlist, list) and vlist:
        vcat = []
        for i, name in enumerate(vlist, 1):
            vcat.append({"barcode": f"V{i:03d}", "name": str(name), "description": "", "model": "", "category": "Vehicles", "location": ""})
        upsert_vehicles(vcat)

def upsert_vehicles(vehicles: List[Dict[str, Any]]) -> None:
    if backend() == "sqlite":
        conn = _sqlite_conn()
        for v in vehicles:
            bc = str(v.get("barcode", "")).strip()
            if not bc:
                continue
            conn.execute("""
              INSERT INTO vehicles(barcode,name,description,model,category,location)
              VALUES(?,?,?,?,?,?)
              ON CONFLICT(barcode) DO UPDATE SET
                name=excluded.name,
                description=excluded.description,
                model=excluded.model,
                category=excluded.category,
                location=excluded.location;
            """, (bc, v.get("name"), v.get("description"), v.get("model"), v.get("category"), v.get("location")))
    else:
        s = _sf()
        s.sql("DELETE FROM PUMA_VEHICLES;").collect()
        ts = datetime.utcnow()
        rows = []
        for v in vehicles:
            bc = str(v.get("barcode","")).strip()
            if not bc:
                continue
            rows.append({
                "BARCODE": bc,
                "NAME": str(v.get("name","")),
                "DESCRIPTION": str(v.get("description","")),
                "MODEL": str(v.get("model","")),
                "CATEGORY": str(v.get("category","")),
                "LOCATION": str(v.get("location","")),
                "UPDATED_AT": ts,
            })
        if rows:
            s.create_dataframe(rows).write.save_as_table("PUMA_VEHICLES", mode="append")

def get_shift(shift_date: str, username: str) -> Optional[Dict[str, Any]]:
    if backend() == "sqlite":
        conn = _sqlite_conn()
        r = conn.execute(
            "SELECT * FROM shifts WHERE shift_date=? AND username=? LIMIT 1;",
            (shift_date, username),
        ).fetchone()
        return dict(r) if r else None

    s = _sf()
    rows = s.sql(
        f"SELECT * FROM PUMA_SHIFTS WHERE SHIFT_DATE=TO_DATE({_q(shift_date)}) AND USERNAME={_q(username)} LIMIT 1"
    ).collect()
    if not rows:
        return None
    d = rows[0].as_dict()
    return {str(k).lower(): v for k, v in d.items()}

def upsert_shift(d: Dict[str, Any]) -> Dict[str, Any]:
    req = ["shift_date","username","client","site","job_number","vehicle_barcode","vehicle_name","shift_start"]
    for k in req:
        if not str(d.get(k,"")).strip():
            raise ValueError(f"Missing required field: {k}")

    if backend() == "sqlite":
        conn = _sqlite_conn()
        ts = now_iso()
        existing = get_shift(d["shift_date"], d["username"])
        if existing:
            conn.execute("""
              UPDATE shifts SET
                client=?, site=?, site_other=?, job_number=?,
                vehicle_barcode=?, vehicle_name=?, vehicle_category=?,
                vehicle_location_expected=?, vehicle_location_actual=?, vehicle_location_mismatch=?,
                shift_start=?, shift_hours=?, shift_notes=?,
                updated_at=?
              WHERE shift_date=? AND username=?;
            """, (
                d.get("client"), d.get("site"), d.get("site_other"), d.get("job_number"),
                d.get("vehicle_barcode"), d.get("vehicle_name"), d.get("vehicle_category"),
                d.get("vehicle_location_expected"), d.get("vehicle_location_actual"), int(bool(d.get("vehicle_location_mismatch"))),
                d.get("shift_start"), float(d.get("shift_hours", 12)), d.get("shift_notes"),
                ts, d["shift_date"], d["username"],
            ))
        else:
            conn.execute("""
              INSERT INTO shifts(
                shift_date, username, client, site, site_other, job_number,
                vehicle_barcode, vehicle_name, vehicle_category,
                vehicle_location_expected, vehicle_location_actual, vehicle_location_mismatch,
                shift_start, shift_hours, shift_notes, created_at, updated_at
              ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
            """, (
                d.get("shift_date"), d.get("username"), d.get("client"), d.get("site"), d.get("site_other"), d.get("job_number"),
                d.get("vehicle_barcode"), d.get("vehicle_name"), d.get("vehicle_category"),
                d.get("vehicle_location_expected"), d.get("vehicle_location_actual"), int(bool(d.get("vehicle_location_mismatch"))),
                d.get("shift_start"), float(d.get("shift_hours", 12)), d.get("shift_notes"),
                ts, ts
            ))
        out = get_shift(d["shift_date"], d["username"])
        assert out is not None
        return out

    s = _sf()
    mismatch = "TRUE" if bool(d.get("vehicle_location_mismatch")) else "FALSE"
    sql = f"""
    MERGE INTO PUMA_SHIFTS t
    USING (SELECT TO_DATE({_q(d['shift_date'])}) SHIFT_DATE, {_q(d['username'])} USERNAME) src
    ON t.SHIFT_DATE=src.SHIFT_DATE AND t.USERNAME=src.USERNAME
    WHEN MATCHED THEN UPDATE SET
      CLIENT={_q(d.get('client'))},
      SITE={_q(d.get('site'))},
      SITE_OTHER={_q(d.get('site_other'))},
      JOB_NUMBER={_q(d.get('job_number'))},
      VEHICLE_BARCODE={_q(d.get('vehicle_barcode'))},
      VEHICLE_NAME={_q(d.get('vehicle_name'))},
      VEHICLE_CATEGORY={_q(d.get('vehicle_category'))},
      VEHICLE_LOCATION_EXPECTED={_q(d.get('vehicle_location_expected'))},
      VEHICLE_LOCATION_ACTUAL={_q(d.get('vehicle_location_actual'))},
      VEHICLE_LOCATION_MISMATCH={mismatch},
      SHIFT_START={_q(d.get('shift_start'))},
      SHIFT_HOURS={float(d.get('shift_hours',12))},
      SHIFT_NOTES={_q(d.get('shift_notes'))},
      UPDATED_AT=CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT(
      SHIFT_DATE, USERNAME, CLIENT, SITE, SITE_OTHER, JOB_NUMBER,
      VEHICLE_BARCODE, VEHICLE_NAME, VEHICLE_CATEGORY,
      VEHICLE_LOCATION_EXPECTED, VEHICLE_LOCATION_ACTUAL, VEHICLE_LOCATION_MISMATCH,
      SHIFT_START, SHIFT_HOURS, SHIFT_NOTES, CREATED_AT, UPDATED_AT
    ) VALUES(
      TO_DATE({_q(d['shift_date'])}), {_q(d['username'])},
      {_q(d.get('client'))}, {_q(d.get('site'))}, {_q(d.get('site_other'))}, {_q(d.get('job_number'))},
      {_q(d.get('vehicle_barcode'))}, {_q(d.get('vehicle_name'))}, {_q(d.get('vehicle_category'))},
      {_q(d.get('vehicle_location_expected'))}, {_q(d.get('vehicle_location_actual'))}, {mismatch},
      {_q(d.get('shift_start'))}, {float(d.get('shift_hours',12))}, {_q(d.get('shift_notes'))},
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );
    """
    s.sql(sql).collect()
    out = get_shift(d["shift_date"], d["username"])
    assert out is not None
    return out

def list_activities(shift_date: str, username: str) -> List[Dict[str, Any]]:
    if backend() == "sqlite":
        conn = _sqlite_conn()
        sh = get_shift(shift_date, username)
        if not sh:
            return []
        rows = conn.execute(
            "SELECT * FROM activities WHERE shift_id=? ORDER BY start_ts ASC, id ASC;",
            (sh["id"],)
        ).fetchall()
        return [dict(r) for r in rows]

    s = _sf()
    rows = s.sql(
        f"SELECT * FROM PUMA_ACTIVITIES WHERE SHIFT_DATE=TO_DATE({_q(shift_date)}) AND USERNAME={_q(username)} ORDER BY START_TS ASC, ID ASC"
    ).collect()
    out: List[Dict[str, Any]] = []
    for r in rows:
        d = r.as_dict()
        out.append({str(k).lower(): v for k, v in d.items()})
    return out

def add_activity(shift_date: str, username: str, a: Dict[str, Any]) -> None:
    for k in ["start_ts","end_ts","code","label"]:
        if not str(a.get(k,"")).strip():
            raise ValueError(f"Missing activity field: {k}")

    if backend() == "sqlite":
        conn = _sqlite_conn()
        sh = get_shift(shift_date, username)
        if not sh:
            raise ValueError("Shift does not exist yet.")
        ts = now_iso()
        conn.execute("""
          INSERT INTO activities(shift_id,start_ts,end_ts,code,label,notes,tool,created_at,updated_at)
          VALUES(?,?,?,?,?,?,?,?,?);
        """, (sh["id"], a.get("start_ts"), a.get("end_ts"), a.get("code"), a.get("label"),
              a.get("notes"), a.get("tool"), ts, ts))
        return

    s = _sf()
    sql = f"""
    INSERT INTO PUMA_ACTIVITIES(SHIFT_DATE,USERNAME,START_TS,END_TS,CODE,LABEL,NOTES,TOOL,CREATED_AT,UPDATED_AT)
    VALUES(
      TO_DATE({_q(shift_date)}), {_q(username)},
      TO_TIMESTAMP_NTZ({_q(a.get('start_ts'))}), TO_TIMESTAMP_NTZ({_q(a.get('end_ts'))}),
      {_q(a.get('code'))}, {_q(a.get('label'))},
      {_q(a.get('notes'))}, {_q(a.get('tool'))},
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );
    """
    s.sql(sql).collect()

def delete_activity(shift_date: str, username: str, activity_id: int) -> None:
    if backend() == "sqlite":
        conn = _sqlite_conn()
        sh = get_shift(shift_date, username)
        if not sh:
            return
        conn.execute("DELETE FROM activities WHERE id=? AND shift_id=?;", (int(activity_id), sh["id"]))
        return
    s = _sf()
    s.sql(
        f"DELETE FROM PUMA_ACTIVITIES WHERE ID={int(activity_id)} AND SHIFT_DATE=TO_DATE({_q(shift_date)}) AND USERNAME={_q(username)}"
    ).collect()
"""

app_py = r"""
import json
from datetime import date as date_cls, datetime, timedelta, time as time_cls
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st
import storage

CONFIG = Path("config")
CLIENTS = ["RTIO", "RTC", "FMG", "FMGX", "Roy Hill", "Other"]

def jload(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def load_users() -> List[str]:
    data = jload(CONFIG / "users.json", {"users":["Kyle"]})
    users = data.get("users", data) if isinstance(data, dict) else data
    users = [u.strip() for u in users if isinstance(u, str) and u.strip()]
    return users or ["Kyle"]

def load_catalog() -> Dict[str, Any]:
    return jload(CONFIG / "catalog.json", {"activity_codes":[], "tools":[]})

def load_sites_by_client() -> Dict[str, List[str]]:
    data = jload(CONFIG / "sites_by_client.json", {})
    return data if isinstance(data, dict) else {}

def load_vehicles_catalog() -> Dict[str, Dict[str, str]]:
    # preferred
    data = jload(CONFIG / "vehicles_catalog.json", {})
    if isinstance(data, dict) and isinstance(data.get("vehicles"), list):
        out: Dict[str, Dict[str, str]] = {}
        for v in data["vehicles"]:
            if not isinstance(v, dict):
                continue
            bc = str(v.get("barcode","")).strip()
            if not bc:
                continue
            out[bc] = {
                "barcode": bc,
                "name": str(v.get("name","")).strip(),
                "description": str(v.get("description","")).strip(),
                "model": str(v.get("model","")).strip(),
                "category": str(v.get("category","")).strip(),
                "location": str(v.get("location","")).strip(),
            }
        if out:
            return out
    # fallback old list
    vlist = jload(CONFIG / "vehicles.json", {}).get("vehicles", [])
    out = {}
    if isinstance(vlist, list):
        for i, name in enumerate(vlist, 1):
            out[f"V{i:03d}"] = {"barcode": f"V{i:03d}", "name": str(name), "description":"", "model":"", "category":"Vehicles", "location":""}
    return out

def iso(d: date_cls) -> str:
    return d.isoformat()

def dt_on(d: date_cls, t: time_cls) -> datetime:
    return datetime(d.year, d.month, d.day, t.hour, t.minute)

def _hhmm_to_time(hhmm: str) -> time_cls:
    try:
        h, m = [int(x) for x in (hhmm or "06:00").split(":")]
        return time_cls(h, m)
    except Exception:
        return time_cls(6, 0)

def shift_progress(shift: Dict[str, Any], acts: List[Dict[str, Any]]) -> float:
    d = datetime.fromisoformat(shift["shift_date"]).date()
    h, m = [int(x) for x in str(shift.get("shift_start","06:00")).split(":")]
    start = datetime(d.year, d.month, d.day, h, m)
    end = start + timedelta(hours=float(shift.get("shift_hours", 12)))
    total = max(1, int((end - start).total_seconds() // 60))
    logged = 0
    for a in acts:
        try:
            a0 = datetime.fromisoformat(str(a.get("start_ts")))
            a1 = datetime.fromisoformat(str(a.get("end_ts")))
        except Exception:
            continue
        lo = max(a0, start)
        hi = min(a1, end)
        if hi > lo:
            logged += int((hi - lo).total_seconds() // 60)
    return max(0.0, min(1.0, logged / total))

def css():
    st.markdown("""
    <style>
      .wrap {max-width: 1200px; margin: 0 auto;}
      .card {border:1px solid rgba(255,255,255,0.08); background:rgba(255,255,255,0.02); border-radius:16px; padding:14px;}
      .muted {opacity:0.85;}
      .rowgap {margin-top:10px;}
      .tiny {font-size: 0.9rem;}
    </style>
    """, unsafe_allow_html=True)

@st.cache_resource
def boot():
    storage.init_storage()
    return True

def login(users: List[str]):
    st.markdown("<div class='wrap'>", unsafe_allow_html=True)
    st.title("Project Puma")
    st.caption("Wireline daily diary — **one shift per user per day**.")
    u = st.selectbox("User", users)
    if st.button("Enter", type="primary"):
        st.session_state.username = u
        st.session_state.shift_date = iso(date_cls.today())
        st.session_state.view = "dd"
        st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

def topbar():
    d = datetime.fromisoformat(st.session_state.shift_date).date()
    st.markdown("<div class='wrap'>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1.1, 2.2, 1.2], vertical_alignment="center")
    with c1:
        st.markdown(f"<div class='card'><b>👤 {st.session_state.username}</b></div>", unsafe_allow_html=True)
    with c2:
        a, b, c = st.columns([1, 2, 1], vertical_alignment="center")
        with a:
            if st.button("◀"):
                st.session_state.shift_date = iso(d - timedelta(days=1)); st.session_state.view="dd"; st.rerun()
        with b:
            st.markdown(f"<div class='card' style='text-align:center;'><b>📅 {d.strftime('%a %d %b %Y')}</b></div>", unsafe_allow_html=True)
        with c:
            if st.button("▶"):
                st.session_state.shift_date = iso(d + timedelta(days=1)); st.session_state.view="dd"; st.rerun()
    with c3:
        colA, colB = st.columns([1,1])
        with colA:
            if st.button("Today"):
                st.session_state.shift_date = iso(date_cls.today()); st.session_state.view="dd"; st.rerun()
        with colB:
            if st.button("Switch user"):
                st.session_state.clear(); st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

def shift_form(vehicles: Dict[str, Dict[str, str]], sites_by_client: Dict[str, List[str]], existing: Optional[Dict[str, Any]]=None):
    existing = existing or {}
    d = st.session_state.shift_date
    username = st.session_state.username

    st.markdown("<div class='wrap'>", unsafe_allow_html=True)
    with st.form("shift_form"):
        st.subheader("Shift details")
        col1, col2 = st.columns([1.2, 1.0])
        with col1:
            client = st.selectbox("Client", CLIENTS, index=(CLIENTS.index(existing.get("client","Other")) if existing.get("client") in CLIENTS else 0))
        with col2:
            job = st.text_input("Job number *", value=str(existing.get("job_number","")))

        site_opts = list(sites_by_client.get(client, []))
        if "Other" not in site_opts:
            site_opts.append("Other")
        site = st.selectbox("Site", site_opts, index=(site_opts.index(existing.get("site","Other")) if existing.get("site") in site_opts else 0))

        site_other = ""
        if site == "Other":
            site_other = st.text_input("Site (manual) *", value=str(existing.get("site_other","")))

        st.markdown("**Vehicle**")
        if not vehicles:
            st.error("No vehicles found. Add config/vehicles_catalog.json")
            vbc = "UNSET"
            v = {"name":"UNSET","category":"","location":"","description":"","model":""}
            expected = ""
        else:
            keys = sorted(vehicles.keys(), key=lambda x: (int(x) if x.isdigit() else 999999, x))
            def fmt(bc: str) -> str:
                v = vehicles[bc]
                cat = v.get("category","")
                return f"{bc} — {v.get('name','')} ({cat})" if cat else f"{bc} — {v.get('name','')}"
            current = existing.get("vehicle_barcode")
            idx = keys.index(current) if current in keys else 0
            vbc = st.selectbox("Vehicle *", keys, format_func=fmt, index=idx)
            v = vehicles[vbc]
            if v.get("description") or v.get("model"):
                st.caption(f"{v.get('description','')} {('· ' + v.get('model')) if v.get('model') else ''}".strip(" ·"))
            expected = v.get("location","") or ""
        actual_default = str(existing.get("vehicle_location_actual") or expected)
        actual = st.text_input("Vehicle location (auto-filled, editable)", value=actual_default)
        mismatch = bool(actual.strip()) and bool(expected.strip()) and actual.strip() != expected.strip()
        if mismatch:
            st.warning("Location differs from catalog — will be flagged in DB.")

        start_t = st.time_input("Shift start *", value=_hhmm_to_time(str(existing.get("shift_start","06:00"))))
        notes = st.text_area("Shift notes (optional)", value=str(existing.get("shift_notes","")), height=90)

        ok = st.form_submit_button("Save shift", type="primary")
        if ok:
            errs=[]
            if not job.strip(): errs.append("Job number required.")
            if site == "Other" and not site_other.strip(): errs.append("Manual site required.")
            if vbc == "UNSET": errs.append("Vehicle required.")
            if errs:
                st.error(" ".join(errs))
            else:
                storage.upsert_shift({
                    "shift_date": d,
                    "username": username,
                    "client": client,
                    "site": site,
                    "site_other": site_other.strip() if site=="Other" else None,
                    "job_number": job.strip(),
                    "vehicle_barcode": vbc,
                    "vehicle_name": v.get("name",""),
                    "vehicle_category": v.get("category",""),
                    "vehicle_location_expected": expected,
                    "vehicle_location_actual": actual.strip(),
                    "vehicle_location_mismatch": mismatch,
                    "shift_start": start_t.strftime("%H:%M"),
                    "shift_hours": 12,
                    "shift_notes": notes.strip() if notes.strip() else None,
                })
                st.session_state.view = "dd"
                st.success("Shift saved.")
                st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

def add_activity_form(catalog: Dict[str, Any]):
    codes = [c for c in catalog.get("activity_codes", []) if isinstance(c, dict) and c.get("code")]
    tools = [t for t in catalog.get("tools", []) if isinstance(t, str) and t.strip()]
    if not codes:
        st.error("No activity codes found in config/catalog.json")
        return
    code_list = [c["code"] for c in codes]
    label_by = {c["code"]: c.get("label", c["code"]) for c in codes}

    d = datetime.fromisoformat(st.session_state.shift_date).date()

    st.markdown("<div class='wrap'>", unsafe_allow_html=True)
    with st.form("act_form", clear_on_submit=True):
        st.subheader("Add activity")
        c1, c2, c3 = st.columns([1.0, 1.0, 1.2])
        with c1:
            t0 = st.time_input("Start", value=datetime.now().time().replace(second=0, microsecond=0))
        with c2:
            t1 = st.time_input("End", value=(datetime.now()+timedelta(minutes=30)).time().replace(second=0, microsecond=0))
        with c3:
            code = st.selectbox("Code", code_list)

        st.caption(f"**{code}** — {label_by.get(code, code)}")

        tool = None
        if code in {"LOG","CAL"} and tools:
            tool = st.selectbox("Tool (LOG/CAL only)", tools)

        notes = st.text_area("Notes (optional)", height=90)

        ok = st.form_submit_button("Add activity", type="primary")
        if ok:
            a0 = dt_on(d, t0); a1 = dt_on(d, t1)
            if a1 <= a0:
                st.error("End must be after start.")
            else:
                storage.add_activity(st.session_state.shift_date, st.session_state.username, {
                    "start_ts": a0.isoformat(timespec="seconds"),
                    "end_ts": a1.isoformat(timespec="seconds"),
                    "code": code,
                    "label": label_by.get(code, code),
                    "tool": tool,
                    "notes": notes.strip() if notes.strip() else None,
                })
                st.session_state.view = "dd"
                st.success("Activity added.")
                st.rerun()

    with st.expander("Activity codes cheat-sheet", expanded=False):
        for c in codes:
            st.markdown(f"- **{c['code']}** — {c.get('label','')}")
    st.markdown("</div>", unsafe_allow_html=True)

def main():
    st.set_page_config(page_title="Project Puma", layout="wide")
    boot()
    css()

    users = load_users()
    catalog = load_catalog()
    sites_by_client = load_sites_by_client()
    vehicles = load_vehicles_catalog()

    if "username" not in st.session_state:
        login(users)
        return

    topbar()
    st.divider()

    sh = storage.get_shift(st.session_state.shift_date, st.session_state.username)
    if sh is None:
        shift_form(vehicles, sites_by_client)
        return

    site_disp = sh.get("site_other") if sh.get("site")=="Other" and sh.get("site_other") else sh.get("site")

    st.markdown("<div class='wrap'>", unsafe_allow_html=True)
    left, right = st.columns([2.2, 1.0], vertical_alignment="top")
    with left:
        st.markdown(f"""
        <div class="card">
          <div class="muted tiny"><b>Client:</b> {sh.get('client')} &nbsp;·&nbsp; <b>Site:</b> {site_disp} &nbsp;·&nbsp; <b>Job #:</b> {sh.get('job_number')}</div>
          <div class="muted tiny" style="margin-top:6px;"><b>Vehicle:</b> {sh.get('vehicle_name')} (#{sh.get('vehicle_barcode')}) &nbsp;·&nbsp; <b>Start:</b> {sh.get('shift_start')} &nbsp;·&nbsp; <b>Hours:</b> {sh.get('shift_hours')}</div>
        </div>
        """, unsafe_allow_html=True)
        if sh.get("vehicle_location_mismatch"):
            st.warning(f"Vehicle location mismatch flagged. Expected: {sh.get('vehicle_location_expected')} · Actual: {sh.get('vehicle_location_actual')}")
    with right:
        if st.button("Edit shift"):
            st.session_state.view = "edit_shift"
        if st.button("Add activity", type="primary"):
            st.session_state.view = "add_activity"
    st.markdown("</div>", unsafe_allow_html=True)

    acts = storage.list_activities(st.session_state.shift_date, st.session_state.username)

    st.markdown("<div class='wrap'>", unsafe_allow_html=True)
    st.markdown("### Shift coverage")
    st.progress(shift_progress(sh, acts))
    st.caption("Fills as activities cover time inside the 12-hour shift window.")
    st.markdown("</div>", unsafe_allow_html=True)

    if st.session_state.get("view") == "edit_shift":
        st.divider()
        shift_form(vehicles, sites_by_client, existing=sh)

    if st.session_state.get("view") == "add_activity":
        st.divider()
        add_activity_form(catalog)

    st.divider()
    st.markdown("<div class='wrap'>", unsafe_allow_html=True)
    st.markdown("## Activities")
    if not acts:
        st.info("No activities yet.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    for a in acts:
        with st.container(border=True):
            st.markdown(f"**{a.get('start_ts')} → {a.get('end_ts')}**  ·  **{a.get('code')}** — {a.get('label')}")
            if a.get("tool"):
                st.caption(f"Tool: {a.get('tool')}")
            if a.get("notes"):
                st.write(a.get("notes"))
            if st.button("Delete", key=f"del_{a.get('id')}"):
                storage.delete_activity(st.session_state.shift_date, st.session_state.username, int(a["id"]))
                st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
"""

readme = r"""
# Project Puma — Daily Diary (Wireline Activity Log)

## What this app does
- Operator selects their **user** (temporary login)
- Each user creates **ONE shift per day**
- Each shift requires:
  - **Client**
  - **Site** (site list depends on client; if missing, choose **Other** and type it)
  - **Job number**
  - **Vehicle** (barcode + details from `vehicles_catalog.json`)
- Each shift has **multiple activities**
- The **coverage bar** fills as activities cover time inside the 12-hour shift window.

## Backends
- Local dev: **SQLite** at `data/project_puma.db`
- Snowflake Streamlit: uses active Snowpark session and creates `PUMA_*` tables.

## Config files
- `config/users.json` → `{"users":["Kyle","..."]}`
- `config/catalog.json` → activity codes + tools
- `config/sites_by_client.json` → site options per client
- `config/vehicles_catalog.json` → list of vehicle objects:
  - `barcode`, `name`, `description`, `model`, `category`, `location`

## Data model (SQLite)
**vehicles**
- `barcode` (PK)
- `name`, `description`, `model`, `category`, `location`

**shifts** (one per user/day; enforced by unique index)
- `id` (PK)
- `shift_date` (YYYY-MM-DD)
- `username`
- `client`, `site`, `site_other`
- `job_number`
- `vehicle_barcode`, `vehicle_name`, `vehicle_category`
- `vehicle_location_expected`, `vehicle_location_actual`
- `vehicle_location_mismatch` (0/1)
- `shift_start` (HH:MM), `shift_hours` (default 12)
- `shift_notes`
- `created_at`, `updated_at`

**activities** (many per shift)
- `id` (PK)
- `shift_id` (FK → shifts.id)
- `start_ts`, `end_ts` (ISO, e.g. 2025-12-16T07:00:00)
- `code`, `label`
- `tool` (only for LOG/CAL)
- `notes`
- `created_at`, `updated_at`

Relationships:
- shifts (1) → (many) activities
- vehicles (1) → (many) shifts (vehicle snapshot stored in shifts)

## Migration / why you saw “no such column: start_ts”
Your old SQLite DB had an `activities` table without `start_ts`. The new storage layer:
- Adds `start_ts/end_ts` if missing
- Backfills from legacy `start_time/end_time` when available
- Creates the index only after columns exist

## Run locally
```bash
source .venv/bin/activate
streamlit run app.py

