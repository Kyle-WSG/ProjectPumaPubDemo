import os
import json
import sqlite3
import time
import uuid
from datetime import datetime, date
from typing import Any, Dict, List, Optional

DB_PATH = os.path.join("data", "project_puma.db")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _normalize_hole_id(hole_id: Any) -> Optional[str]:
    if hole_id is None:
        return None
    val = str(hole_id).strip()
    return val if val else None


def _generate_hole_id() -> str:
    return str(uuid.uuid4())


def _try_get_sf_session():
    try:
        from snowflake.snowpark.context import get_active_session  # type: ignore
        return get_active_session()
    except Exception:
        return None


def backend() -> str:
    return "snowflake" if _try_get_sf_session() is not None else "sqlite"


def _sf_ensure_hole(s, hole_id: Optional[str]) -> None:
    if not hole_id:
        return
    s.sql(
        """
        MERGE INTO PUMA_HOLES t
        USING (SELECT ? HOLE_ID) src
        ON t.HOLE_ID = src.HOLE_ID
        WHEN MATCHED THEN UPDATE SET UPDATED_AT=CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (HOLE_ID, CREATED_AT, UPDATED_AT)
        VALUES(?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
        """,
        params=[hole_id, hole_id],
    ).collect()


def _load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


# ---------------- SQLITE ----------------
def _sqlite_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    # Enable WAL with retries in case another process briefly locks
    for i in range(30):
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(0.15 * (i + 1))
                continue
            raise
    return conn


def _sqlite_cols(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return [r[1] for r in rows]


def _sqlite_col_defaults(conn: sqlite3.Connection, table: str) -> Dict[str, Any]:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return {r[1]: r[4] for r in rows}


def _sqlite_has_hole_fk(conn: sqlite3.Connection) -> bool:
    try:
        rows = conn.execute("PRAGMA foreign_key_list(activities);").fetchall()
    except Exception:
        return False
    for r in rows:
        if len(r) >= 4 and r[2] == "holes" and r[3] == "hole_id":
            return True
    return False


def _sqlite_ensure_holes_table(conn: sqlite3.Connection) -> None:
    _retry_locked(
        lambda: conn.execute(
            """
            CREATE TABLE IF NOT EXISTS holes(
              hole_id TEXT PRIMARY KEY,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            """
        )
    )


def _sqlite_upsert_hole(conn: sqlite3.Connection, hole_id: str, ts: str) -> None:
    if not hole_id:
        return
    _retry_locked(
        lambda: conn.execute(
            """
            INSERT INTO holes(hole_id, created_at, updated_at)
            VALUES(?,?,?)
            ON CONFLICT(hole_id) DO UPDATE SET updated_at=excluded.updated_at;
            """,
            (hole_id, ts, ts),
        )
    )


def _retry_locked(fn, retries: int = 30):
    for i in range(retries):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(0.1 * (i + 1))
                continue
            raise
    raise sqlite3.OperationalError("database is locked (retry exhausted)")


def _sqlite_activity_schema(conn: sqlite3.Connection) -> str:
    cols = set(_sqlite_cols(conn, "activities"))
    if "start_ts" in cols:
        return "new"
    if "start_time" in cols:
        return "legacy"
    raise sqlite3.OperationalError("activities table schema not recognized")


def _sqlite_dedupe_shifts(conn: sqlite3.Connection) -> None:
    """Keep one shift per (shift_date, username); move activities to the keep_id."""
    try:
        cols = set(_sqlite_cols(conn, "shifts"))
        if "shift_date" not in cols or "username" not in cols:
            return
        dupes = conn.execute(
            "SELECT shift_date, username, COUNT(*) n "
            "FROM shifts GROUP BY shift_date, username HAVING n > 1;"
        ).fetchall()
        for r in dupes:
            shift_date = r[0]
            username = r[1]
            rows = conn.execute(
                "SELECT id FROM shifts WHERE shift_date=? AND username=? "
                "ORDER BY COALESCE(updated_at, created_at, '') DESC, id DESC;",
                (shift_date, username),
            ).fetchall()
            if not rows:
                continue
            keep_id = int(rows[0][0])
            for r_id in rows[1:]:
                sid = int(r_id[0])
                _retry_locked(lambda: conn.execute("UPDATE activities SET shift_id=? WHERE shift_id=?;", (keep_id, sid)))
                _retry_locked(lambda: conn.execute("DELETE FROM shifts WHERE id=?;", (sid,)))
    except Exception:
        # If the table is old this may fail; don't block app start.
        pass


def _sqlite_migrate_activities_to_new(conn: sqlite3.Connection) -> None:
    """
    Ensure activities uses the canonical schema (start_ts/end_ts and no legacy start_time/end_time).
    If legacy columns are present or required columns are missing, rebuild the table and copy data.
    """
    cols = set(_sqlite_cols(conn, "activities"))
    needs_migration = (
        "start_ts" not in cols
        or "end_ts" not in cols
        or "code" not in cols
        or "label" not in cols
        or "hole_id" not in cols
        or "start_time" in cols
        or "end_time" in cols
    )
    if not _sqlite_has_hole_fk(conn):
        needs_migration = True
    _sqlite_ensure_holes_table(conn)
    if not needs_migration:
        try:
            rows = conn.execute("SELECT id, code, hole_id, created_at, updated_at FROM activities;").fetchall()
        except Exception:
            return
        now_ts = _now()
        for r in rows:
            d = dict(r)
            hole_id_val = _normalize_hole_id(d.get("hole_id"))
            code_val = d.get("code")
            if code_val == "LOG" and not hole_id_val:
                hole_id_val = _generate_hole_id()
                _retry_locked(
                    lambda: conn.execute(
                        "UPDATE activities SET hole_id=?, updated_at=? WHERE id=?;",
                        (hole_id_val, now_ts, int(d.get("id"))),
                    )
                )
            ts = d.get("updated_at") or d.get("created_at") or now_ts
            if hole_id_val:
                _sqlite_upsert_hole(conn, hole_id_val, ts)
        return

    _retry_locked(lambda: conn.execute("DROP TABLE IF EXISTS activities_new;"))
    _retry_locked(
        lambda: conn.execute(
            """
            CREATE TABLE activities_new(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              shift_id INTEGER NOT NULL,
              start_ts TEXT NOT NULL,
              end_ts TEXT NOT NULL,
              code TEXT NOT NULL,
              label TEXT NOT NULL,
              notes TEXT,
              tool TEXT,
              hole_id TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              FOREIGN KEY(shift_id) REFERENCES shifts(id) ON DELETE CASCADE,
              FOREIGN KEY(hole_id) REFERENCES holes(hole_id) ON DELETE SET NULL
            );
            """
        )
    )

    now_ts = _now()
    try:
        rows = conn.execute("SELECT * FROM activities;").fetchall()
    except Exception:
        rows = []
    for r in rows:
        d = dict(r)
        start_val = d.get("start_ts") or d.get("start_time") or d.get("start") or d.get("begin_time")
        end_val = d.get("end_ts") or d.get("end_time") or d.get("end") or d.get("finish_time") or start_val
        if not start_val:
            start_val = now_ts
        if not end_val:
            end_val = start_val
        label_val = d.get("label") or d.get("title") or d.get("description") or "Activity"
        notes_val = d.get("notes") or d.get("comments")
        tool_val = d.get("tool") or d.get("tool_ref") or d.get("tools_csv")
        hole_id_val = _normalize_hole_id(d.get("hole_id"))
        code_val = d.get("code") or "OTH"
        if code_val == "LOG" and not hole_id_val:
            hole_id_val = _generate_hole_id()
        created_at = d.get("created_at") or now_ts
        updated_at = d.get("updated_at") or created_at
        if hole_id_val:
            _sqlite_upsert_hole(conn, hole_id_val, updated_at)
        _retry_locked(
            lambda: conn.execute(
                """
                INSERT INTO activities_new(shift_id, start_ts, end_ts, code, label, notes, tool, hole_id, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?);
                """,
                (
                    d.get("shift_id"),
                    start_val,
                    end_val,
                    code_val,
                    label_val,
                    notes_val,
                    tool_val,
                    hole_id_val,
                    created_at,
                    updated_at,
                ),
            )
        )

    _retry_locked(lambda: conn.execute("DROP TABLE IF EXISTS activities;"))
    _retry_locked(lambda: conn.execute("ALTER TABLE activities_new RENAME TO activities;"))
    _retry_locked(lambda: conn.execute("CREATE INDEX IF NOT EXISTS idx_acts_shift_start ON activities(shift_id, start_ts);"))
    _retry_locked(lambda: conn.execute("CREATE INDEX IF NOT EXISTS idx_acts_hole_id ON activities(hole_id);"))


def _sqlite_migrate_shifts_to_new(conn: sqlite3.Connection) -> None:
    cols = set(_sqlite_cols(conn, "shifts"))
    if not cols:
        return

    canonical_cols = {
        "id",
        "shift_date",
        "username",
        "client",
        "site",
        "site_other",
        "job_number",
        "vehicle_barcode",
        "vehicle_name",
        "vehicle_description",
        "vehicle_model",
        "vehicle_category",
        "vehicle_location_expected",
        "vehicle_location_actual",
        "vehicle_location_mismatch",
        "shift_start",
        "shift_hours",
        "shift_notes",
        "created_at",
        "updated_at",
    }

    missing = canonical_cols - cols
    extras = cols - canonical_cols
    if not missing and not extras:
        return

    defaults = _sqlite_col_defaults(conn, "shifts")
    default_username = defaults.get("username")
    if isinstance(default_username, str):
        default_username = default_username.strip()
        if len(default_username) >= 2 and default_username[0] == default_username[-1] and default_username[0] in {"'", "\""}:
            default_username = default_username[1:-1]

    rows = conn.execute("SELECT * FROM shifts;").fetchall()
    now_ts = _now()

    def clean(val: Any) -> str:
        return str(val).strip() if val is not None else ""

    def coerce_date(val: Any) -> str:
        s = clean(val)
        if not s:
            return date.today().isoformat()
        try:
            return datetime.fromisoformat(s).date().isoformat()
        except Exception:
            if len(s) >= 10:
                try:
                    return datetime.fromisoformat(s[:10]).date().isoformat()
                except Exception:
                    pass
        return date.today().isoformat()

    def coerce_time(val: Any) -> str:
        s = clean(val)
        if not s:
            return "06:00"
        if "T" in s:
            try:
                return datetime.fromisoformat(s).strftime("%H:%M")
            except Exception:
                pass
        parts = s.split(":")
        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
            return f"{int(parts[0]):02d}:{int(parts[1]):02d}"
        return "06:00"

    def coerce_hours(val: Any) -> float:
        try:
            hours = float(val)
        except Exception:
            hours = 12.0
        if hours <= 0:
            hours = 12.0
        return hours

    def parse_ts(val: Any) -> Optional[datetime]:
        if val is None:
            return None
        try:
            return datetime.fromisoformat(str(val))
        except Exception:
            return None

    def to_int_bool(val: Any) -> int:
        if val is None:
            return 0
        try:
            return 1 if int(val) else 0
        except Exception:
            return 1 if str(val).strip().lower() in {"true", "yes", "y", "1"} else 0

    canonical_by_id: Dict[int, Dict[str, Any]] = {}
    keep_by_key: Dict[tuple[str, str], int] = {}
    best_key: Dict[tuple[str, str], tuple[datetime, int]] = {}

    for r in rows:
        d = dict(r)
        if d.get("id") is None:
            continue
        old_id = int(d.get("id"))

        username = clean(d.get("username"))
        active_user = clean(d.get("active_user"))
        if not username and active_user:
            username = active_user
        if active_user and default_username and username == default_username:
            username = active_user
        if not username:
            username = "UNKNOWN"

        shift_date = coerce_date(d.get("shift_date"))
        client = clean(d.get("client")) or "Other"

        site = clean(d.get("site"))
        site_other = clean(d.get("site_other"))
        site_name = clean(d.get("site_name"))
        if not site and site_name:
            site = "Other"
            if not site_other:
                site_other = site_name
        if site in {"Other", "Other (manual)"} and site_name and not site_other:
            site_other = site_name
        if not site:
            site = "Other"

        job_number = clean(d.get("job_number")) or "UNKNOWN"

        vehicle_barcode = clean(d.get("vehicle_barcode"))
        vehicle_name = clean(d.get("vehicle_name"))
        legacy_vehicle = clean(d.get("vehicle"))
        if not vehicle_barcode and legacy_vehicle:
            vehicle_barcode = legacy_vehicle
        if not vehicle_name and legacy_vehicle:
            vehicle_name = legacy_vehicle
        if not vehicle_barcode:
            vehicle_barcode = "UNSET"
        if not vehicle_name:
            vehicle_name = "Vehicle"

        created_at = clean(d.get("created_at")) or now_ts
        updated_at = clean(d.get("updated_at")) or created_at

        canon = {
            "id": old_id,
            "shift_date": shift_date,
            "username": username,
            "client": client,
            "site": site,
            "site_other": site_other or None,
            "job_number": job_number,
            "vehicle_barcode": vehicle_barcode,
            "vehicle_name": vehicle_name,
            "vehicle_description": clean(d.get("vehicle_description")) or None,
            "vehicle_model": clean(d.get("vehicle_model")) or None,
            "vehicle_category": clean(d.get("vehicle_category")) or None,
            "vehicle_location_expected": clean(d.get("vehicle_location_expected")) or None,
            "vehicle_location_actual": clean(d.get("vehicle_location_actual")) or None,
            "vehicle_location_mismatch": to_int_bool(d.get("vehicle_location_mismatch")),
            "shift_start": coerce_time(d.get("shift_start")),
            "shift_hours": coerce_hours(d.get("shift_hours")),
            "shift_notes": clean(d.get("shift_notes")) or None,
            "created_at": created_at,
            "updated_at": updated_at,
        }
        canonical_by_id[old_id] = canon

        key = (shift_date, username)
        ts = parse_ts(updated_at) or parse_ts(created_at) or datetime.min
        sort_key = (ts, old_id)
        if key not in best_key or sort_key > best_key[key]:
            best_key[key] = sort_key
            keep_by_key[key] = old_id

    conn.execute("PRAGMA foreign_keys=OFF;")
    try:
        _retry_locked(lambda: conn.execute("DROP TABLE IF EXISTS shifts_new;"))
        _retry_locked(
            lambda: conn.execute(
                """
                CREATE TABLE shifts_new(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  shift_date TEXT NOT NULL,
                  username TEXT NOT NULL,
                  client TEXT NOT NULL,
                  site TEXT NOT NULL,
                  site_other TEXT,
                  job_number TEXT NOT NULL,
                  vehicle_barcode TEXT NOT NULL,
                  vehicle_name TEXT NOT NULL,
                  vehicle_description TEXT,
                  vehicle_model TEXT,
                  vehicle_category TEXT,
                  vehicle_location_expected TEXT,
                  vehicle_location_actual TEXT,
                  vehicle_location_mismatch INTEGER NOT NULL DEFAULT 0,
                  shift_start TEXT NOT NULL,
                  shift_hours REAL NOT NULL DEFAULT 12,
                  shift_notes TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );
                """
            )
        )

        if canonical_by_id:
            insert_cols = [
                "id",
                "shift_date",
                "username",
                "client",
                "site",
                "site_other",
                "job_number",
                "vehicle_barcode",
                "vehicle_name",
                "vehicle_description",
                "vehicle_model",
                "vehicle_category",
                "vehicle_location_expected",
                "vehicle_location_actual",
                "vehicle_location_mismatch",
                "shift_start",
                "shift_hours",
                "shift_notes",
                "created_at",
                "updated_at",
            ]
            placeholders = ",".join(["?"] * len(insert_cols))
            insert_sql = f"INSERT INTO shifts_new({','.join(insert_cols)}) VALUES({placeholders});"

            for keep_id in keep_by_key.values():
                canon = canonical_by_id.get(keep_id)
                if not canon:
                    continue
                values = [canon.get(col) for col in insert_cols]
                _retry_locked(lambda: conn.execute(insert_sql, values))

            for old_id, canon in canonical_by_id.items():
                keep_id = keep_by_key.get((canon["shift_date"], canon["username"]))
                if keep_id is None or old_id == keep_id:
                    continue
                _retry_locked(lambda: conn.execute("UPDATE activities SET shift_id=? WHERE shift_id=?;", (keep_id, old_id)))

        _retry_locked(lambda: conn.execute("DROP TABLE IF EXISTS shifts;"))
        _retry_locked(lambda: conn.execute("ALTER TABLE shifts_new RENAME TO shifts;"))
    finally:
        conn.execute("PRAGMA foreign_keys=ON;")

def _init_sqlite() -> None:
    c = _sqlite_conn()

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS vehicles(
          barcode TEXT PRIMARY KEY,
          name TEXT,
          description TEXT,
          model TEXT,
          category TEXT,
          location TEXT
        );
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS holes(
          hole_id TEXT PRIMARY KEY,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS shifts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          shift_date TEXT NOT NULL,
          username TEXT NOT NULL,
          client TEXT NOT NULL,
          site TEXT NOT NULL,
          site_other TEXT,
          job_number TEXT NOT NULL,
          vehicle_barcode TEXT NOT NULL,
          vehicle_name TEXT NOT NULL,
          vehicle_description TEXT,
          vehicle_model TEXT,
          vehicle_category TEXT,
          vehicle_location_expected TEXT,
          vehicle_location_actual TEXT,
          vehicle_location_mismatch INTEGER NOT NULL DEFAULT 0,
          shift_start TEXT NOT NULL,
          shift_hours REAL NOT NULL DEFAULT 12,
          shift_notes TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS activities(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          shift_id INTEGER NOT NULL,
          start_ts TEXT NOT NULL,
          end_ts TEXT NOT NULL,
          code TEXT NOT NULL,
          label TEXT NOT NULL,
          notes TEXT,
          tool TEXT,
          hole_id TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          FOREIGN KEY(shift_id) REFERENCES shifts(id) ON DELETE CASCADE,
          FOREIGN KEY(hole_id) REFERENCES holes(hole_id) ON DELETE SET NULL
        );
        """
    )

    _sqlite_migrate_shifts_to_new(c)
    _sqlite_migrate_activities_to_new(c)
    _sqlite_dedupe_shifts(c)

    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_shifts_user_day ON shifts(shift_date, username);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_shifts_user_date ON shifts(username, shift_date);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_acts_shift_start ON activities(shift_id, start_ts);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_acts_hole_id ON activities(hole_id);")
    c.commit()
    c.close()


def init_storage() -> None:
    if backend() == "snowflake":
        s = _try_get_sf_session()
        assert s is not None
        s.sql(
            """
            CREATE TABLE IF NOT EXISTS PUMA_VEHICLES(
              BARCODE STRING, NAME STRING, DESCRIPTION STRING, MODEL STRING,
              CATEGORY STRING, LOCATION STRING, UPDATED_AT TIMESTAMP_NTZ
            );
            """
        ).collect()

        s.sql(
            """
            CREATE TABLE IF NOT EXISTS PUMA_SHIFTS(
              SHIFT_DATE DATE, USERNAME STRING,
              CLIENT STRING, SITE STRING, SITE_OTHER STRING, JOB_NUMBER STRING,
              VEHICLE_BARCODE STRING, VEHICLE_NAME STRING, VEHICLE_DESCRIPTION STRING, VEHICLE_MODEL STRING, VEHICLE_CATEGORY STRING,
              VEHICLE_LOCATION_EXPECTED STRING, VEHICLE_LOCATION_ACTUAL STRING, VEHICLE_LOCATION_MISMATCH BOOLEAN,
              SHIFT_START STRING, SHIFT_HOURS FLOAT, SHIFT_NOTES STRING,
              CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ
            );
            """
        ).collect()

        s.sql(
            """
            CREATE TABLE IF NOT EXISTS PUMA_HOLES(
              HOLE_ID STRING,
              CREATED_AT TIMESTAMP_NTZ,
              UPDATED_AT TIMESTAMP_NTZ
            );
            """
        ).collect()

        s.sql(
            """
            CREATE TABLE IF NOT EXISTS PUMA_ACTIVITIES(
              ID NUMBER AUTOINCREMENT,
              SHIFT_DATE DATE, USERNAME STRING,
              START_TS TIMESTAMP_NTZ, END_TS TIMESTAMP_NTZ,
              CODE STRING, LABEL STRING, NOTES STRING, TOOL STRING, HOLE_ID STRING,
              CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ
            );
            """
        ).collect()
        try:
            s.sql("ALTER TABLE PUMA_ACTIVITIES ADD COLUMN HOLE_ID STRING;").collect()
        except Exception:
            pass
        return

    _init_sqlite()


def upsert_reference_data(vehicles: List[Dict[str, Any]]) -> None:
    if not vehicles:
        return

    if backend() == "snowflake":
        s = _try_get_sf_session()
        assert s is not None
        s.sql("DELETE FROM PUMA_VEHICLES;").collect()
        ts = datetime.utcnow()
        rows = []
        for v in vehicles:
            rows.append({
                "BARCODE": str(v.get("barcode", "")),
                "NAME": str(v.get("name", "")),
                "DESCRIPTION": str(v.get("description", "")),
                "MODEL": str(v.get("model", "")),
                "CATEGORY": str(v.get("category", "")),
                "LOCATION": str(v.get("location", "")),
                "UPDATED_AT": ts,
            })
        if rows:
            s.create_dataframe(rows).write.save_as_table("PUMA_VEHICLES", mode="append")
        return

    conn = _sqlite_conn()
    for v in vehicles:
        _retry_locked(
            lambda: conn.execute(
                """
                INSERT INTO vehicles(barcode, name, description, model, category, location)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(barcode) DO UPDATE SET
                  name=excluded.name,
                  description=excluded.description,
                  model=excluded.model,
                  category=excluded.category,
                  location=excluded.location;
                """,
                (
                    v.get("barcode"),
                    v.get("name"),
                    v.get("description"),
                    v.get("model"),
                    v.get("category"),
                    v.get("location"),
                ),
            )
        )
    conn.commit()
    conn.close()


def _sqlite_get_shift(conn: sqlite3.Connection, shift_date: str, username: str) -> Optional[Dict[str, Any]]:
    cols = set(_sqlite_cols(conn, "shifts"))
    row = None
    if "username" in cols:
        row = conn.execute(
            "SELECT * FROM shifts WHERE shift_date=? AND username=? ORDER BY updated_at DESC, id DESC LIMIT 1;",
            (shift_date, username),
        ).fetchone()
    if row is None and "active_user" in cols:
        row = conn.execute(
            "SELECT * FROM shifts WHERE shift_date=? AND active_user=? ORDER BY updated_at DESC, id DESC LIMIT 1;",
            (shift_date, username),
        ).fetchone()
    return dict(row) if row else None


def get_shift(shift_date: date | str, username: str) -> Optional[Dict[str, Any]]:
    dt_str = shift_date.isoformat() if isinstance(shift_date, date) else str(shift_date)

    if backend() == "snowflake":
        s = _try_get_sf_session()
        assert s is not None
        rows = s.sql(
            "SELECT * FROM PUMA_SHIFTS WHERE SHIFT_DATE=? AND USERNAME=? ORDER BY UPDATED_AT DESC LIMIT 1",
            params=[dt_str, username],
        ).collect()
        if not rows:
            return None
        r = rows[0].as_dict()
        r = {k.lower(): v for k, v in r.items()}
        r["shift_date"] = dt_str
        return r

    conn = _sqlite_conn()
    row = _sqlite_get_shift(conn, dt_str, username)
    conn.close()
    return row


def upsert_shift(d: Dict[str, Any]) -> Dict[str, Any]:
    required = ["shift_date", "username", "client", "site", "job_number", "vehicle_barcode", "vehicle_name", "shift_start"]
    for k in required:
        if not str(d.get(k, "")).strip():
            raise ValueError(f"Missing required field: {k}")

    dt_str = d.get("shift_date") if isinstance(d.get("shift_date"), str) else d.get("shift_date").isoformat()

    if backend() == "snowflake":
        s = _try_get_sf_session()
        assert s is not None
        ts = "CURRENT_TIMESTAMP()"
        mismatch = "TRUE" if bool(d.get("vehicle_location_mismatch")) else "FALSE"
        s.sql(
            """
            MERGE INTO PUMA_SHIFTS t
            USING (SELECT TO_DATE(?) SHIFT_DATE, ? USERNAME) src
            ON t.SHIFT_DATE = src.SHIFT_DATE AND t.USERNAME = src.USERNAME
            WHEN MATCHED THEN UPDATE SET
              CLIENT=?, SITE=?, SITE_OTHER=?, JOB_NUMBER=?,
              VEHICLE_BARCODE=?, VEHICLE_NAME=?, VEHICLE_DESCRIPTION=?, VEHICLE_MODEL=?, VEHICLE_CATEGORY=?,
              VEHICLE_LOCATION_EXPECTED=?, VEHICLE_LOCATION_ACTUAL=?, VEHICLE_LOCATION_MISMATCH=?,
              SHIFT_START=?, SHIFT_HOURS=?, SHIFT_NOTES=?, UPDATED_AT={ts}
            WHEN NOT MATCHED THEN INSERT(
              SHIFT_DATE, USERNAME, CLIENT, SITE, SITE_OTHER, JOB_NUMBER,
              VEHICLE_BARCODE, VEHICLE_NAME, VEHICLE_DESCRIPTION, VEHICLE_MODEL, VEHICLE_CATEGORY,
              VEHICLE_LOCATION_EXPECTED, VEHICLE_LOCATION_ACTUAL, VEHICLE_LOCATION_MISMATCH,
              SHIFT_START, SHIFT_HOURS, SHIFT_NOTES, CREATED_AT, UPDATED_AT
            ) VALUES(
              TO_DATE(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {ts}, {ts}
            );
            """.format(ts=ts),
            params=[
                dt_str,
                d.get("username"),
                d.get("client"), d.get("site"), d.get("site_other"), d.get("job_number"),
                d.get("vehicle_barcode"), d.get("vehicle_name"), d.get("vehicle_description"), d.get("vehicle_model"), d.get("vehicle_category"),
                d.get("vehicle_location_expected"), d.get("vehicle_location_actual"), bool(d.get("vehicle_location_mismatch")),
                d.get("shift_start"), float(d.get("shift_hours", 12)), d.get("shift_notes"),
                dt_str,
                d.get("username"), d.get("client"), d.get("site"), d.get("site_other"), d.get("job_number"),
                d.get("vehicle_barcode"), d.get("vehicle_name"), d.get("vehicle_description"), d.get("vehicle_model"), d.get("vehicle_category"),
                d.get("vehicle_location_expected"), d.get("vehicle_location_actual"), bool(d.get("vehicle_location_mismatch")),
                d.get("shift_start"), float(d.get("shift_hours", 12)), d.get("shift_notes"),
            ],
        ).collect()
        out = get_shift(dt_str, d.get("username"))
        assert out is not None
        return out

    conn = _sqlite_conn()
    col_set = set(_sqlite_cols(conn, "shifts"))
    existing = _sqlite_get_shift(conn, dt_str, d.get("username"))
    ts = _now()

    payload = {
        "shift_date": dt_str,
        "username": d.get("username"),
        "client": d.get("client"),
        "site": d.get("site"),
        "site_other": d.get("site_other"),
        "job_number": d.get("job_number"),
        "vehicle_barcode": d.get("vehicle_barcode"),
        "vehicle_name": d.get("vehicle_name"),
        "vehicle_description": d.get("vehicle_description"),
        "vehicle_model": d.get("vehicle_model"),
        "vehicle_category": d.get("vehicle_category"),
        "vehicle_location_expected": d.get("vehicle_location_expected"),
        "vehicle_location_actual": d.get("vehicle_location_actual"),
        "vehicle_location_mismatch": int(bool(d.get("vehicle_location_mismatch"))),
        "shift_start": d.get("shift_start"),
        "shift_hours": float(d.get("shift_hours", 12)),
        "shift_notes": d.get("shift_notes"),
        "updated_at": ts,
    }

    if "active_user" in col_set:
        payload["active_user"] = d.get("username")
    if "shift_type" in col_set:
        payload["shift_type"] = d.get("shift_type") or "Day"
    if "vehicle" in col_set:
        payload["vehicle"] = d.get("vehicle") or d.get("vehicle_name") or d.get("vehicle_barcode") or "UNSET"
    if "site_name" in col_set:
        site_name = d.get("site_other") if d.get("site") in {"Other", "Other (manual)"} else d.get("site")
        payload["site_name"] = site_name
    if "synced" in col_set and "synced" not in payload:
        payload["synced"] = int(bool(d.get("synced", 0)))

    payload = {k: v for k, v in payload.items() if k in col_set}

    if existing:
        where_clause = None
        where_params: tuple[Any, ...]
        if existing.get("id") is not None:
            where_clause = "id=?"
            where_params = (int(existing.get("id")),)
        elif "username" in col_set:
            where_clause = "shift_date=? AND username=?"
            where_params = (dt_str, d.get("username"))
        elif "active_user" in col_set:
            where_clause = "shift_date=? AND active_user=?"
            where_params = (dt_str, d.get("username"))
        else:
            where_clause = "shift_date=?"
            where_params = (dt_str,)

        sets = ",".join([f"{k}=?" for k in payload.keys()])
        _retry_locked(lambda: conn.execute(f"UPDATE shifts SET {sets} WHERE {where_clause};", tuple(payload.values()) + where_params))
    else:
        if "created_at" in col_set:
            payload["created_at"] = ts
        insert_cols = ",".join(payload.keys())
        insert_vals = ":" + ",:".join(payload.keys())
        try:
            _retry_locked(lambda: conn.execute(f"INSERT INTO shifts({insert_cols}) VALUES({insert_vals});", payload))
        except sqlite3.IntegrityError:
            # If an unexpected constraint trips (e.g., legacy unique indexes), dedupe and try again.
            _sqlite_dedupe_shifts(conn)
            col_list = list(payload.keys())
            placeholders = ",".join(["?"] * len(col_list))
            try:
                _retry_locked(lambda: conn.execute(f"INSERT OR REPLACE INTO shifts({insert_cols}) VALUES({placeholders});", tuple(payload[c] for c in col_list)))
            except sqlite3.IntegrityError:
                # Final fallback: attempt update in place
                if "username" in col_set:
                    where_clause = "shift_date=? AND username=?"
                    where_params = (dt_str, d.get("username"))
                elif "active_user" in col_set:
                    where_clause = "shift_date=? AND active_user=?"
                    where_params = (dt_str, d.get("username"))
                else:
                    where_clause = "shift_date=?"
                    where_params = (dt_str,)
                _retry_locked(
                    lambda: conn.execute(
                        f"UPDATE shifts SET {','.join([f'{k}=?' for k in payload.keys() if k!='created_at'])} WHERE {where_clause};",
                        tuple(v for k, v in payload.items() if k != "created_at") + where_params,
                    )
                )

    conn.commit()
    out = _sqlite_get_shift(conn, dt_str, d.get("username"))
    conn.close()
    if out is None:
        # Fallback return payload to avoid crashing if retrieval fails unexpectedly
        payload["id"] = payload.get("id")
        payload["created_at"] = payload.get("created_at", ts)
        return payload
    return out


def list_activities(shift_date: str | date, username: str) -> List[Dict[str, Any]]:
    dt_str = shift_date.isoformat() if isinstance(shift_date, date) else str(shift_date)

    if backend() == "snowflake":
        s = _try_get_sf_session()
        assert s is not None
        rows = s.sql(
            "SELECT * FROM PUMA_ACTIVITIES WHERE SHIFT_DATE=TO_DATE(?) AND USERNAME=? ORDER BY START_TS ASC, ID ASC",
            params=[dt_str, username],
        ).collect()
        out = []
        for r in rows:
            d = {k.lower(): v for k, v in r.as_dict().items()}
            for fld in ("start_ts", "end_ts", "created_at", "updated_at"):
                if isinstance(d.get(fld), (datetime, date)):
                    d[fld] = d[fld].isoformat(timespec="seconds")
            out.append(d)
        return out

    conn = _sqlite_conn()
    sh = _sqlite_get_shift(conn, dt_str, username)
    if not sh:
        conn.close()
        return []

    schema = _sqlite_activity_schema(conn)
    if schema == "new":
        rows = conn.execute(
            "SELECT * FROM activities WHERE shift_id=? ORDER BY start_ts ASC, id ASC;",
            (sh["id"],),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            # Hybrid schemas can keep legacy columns; populate missing timeline fields from them.
            if not d.get("start_ts"):
                d["start_ts"] = d.get("start_time") or d.get("start")
            if not d.get("end_ts"):
                d["end_ts"] = d.get("end_time") or d.get("end")
            if not d.get("label"):
                d["label"] = d.get("description") or d.get("title")
            if not d.get("notes"):
                d["notes"] = d.get("comments")
            if not d.get("tool"):
                d["tool"] = d.get("tools_csv") or d.get("tool_ref")
            d.setdefault("hole_id", d.get("hole_id"))
            out.append(d)
    else:
        rows = conn.execute(
            "SELECT * FROM activities WHERE shift_id=? ORDER BY start_time ASC, id ASC;",
            (sh["id"],),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            out.append({
                "id": d.get("id"),
                "shift_id": d.get("shift_id"),
                "start_ts": d.get("start_ts") or d.get("start_time"),
                "end_ts": d.get("end_ts") or d.get("end_time"),
                "code": d.get("code"),
                "label": d.get("label") or d.get("title") or d.get("description"),
                "notes": d.get("notes") or d.get("comments") or "",
                "tool": d.get("tool") or d.get("tool_ref") or d.get("tools_csv") or "",
                "hole_id": d.get("hole_id"),
            })
    conn.close()
    return out


def add_activity(shift_date: str | date, username: str, a: Dict[str, Any]) -> None:
    # Normalize required fields early and guarantee non-NULL start/end for legacy schemas.
    start_val = a.get("start_ts") or a.get("start_time")
    end_val = a.get("end_ts") or a.get("end_time") or start_val
    code_val = a.get("code")
    label_val = a.get("label")
    hole_id_val = _normalize_hole_id(a.get("hole_id"))
    if code_val == "LOG" and not hole_id_val:
        hole_id_val = _generate_hole_id()

    for name, val in (("start_ts", start_val), ("end_ts", end_val), ("code", code_val), ("label", label_val)):
        if val is None or (isinstance(val, str) and not str(val).strip()):
            raise ValueError(f"Missing activity field: {name}")

    dt_str = shift_date.isoformat() if isinstance(shift_date, date) else str(shift_date)

    if backend() == "snowflake":
        s = _try_get_sf_session()
        assert s is not None
        _sf_ensure_hole(s, hole_id_val)
        s.sql(
            """
            INSERT INTO PUMA_ACTIVITIES(
              SHIFT_DATE, USERNAME, START_TS, END_TS, CODE, LABEL, NOTES, TOOL, HOLE_ID, CREATED_AT, UPDATED_AT
            ) VALUES(TO_DATE(?), ?, TO_TIMESTAMP_NTZ(?), TO_TIMESTAMP_NTZ(?), ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
            """,
            params=[dt_str, username, start_val, end_val, code_val, label_val, a.get("notes"), a.get("tool"), hole_id_val],
        ).collect()
        return

    conn = _sqlite_conn()
    sh = _sqlite_get_shift(conn, dt_str, username)
    if not sh:
        conn.close()
        raise ValueError("Shift does not exist yet.")

    ts = _now()
    if hole_id_val:
        _sqlite_upsert_hole(conn, hole_id_val, ts)
    cols = set(_sqlite_cols(conn, "activities"))
    # Populate both new and legacy columns when present to avoid NOT NULL conflicts on hybrid schemas.
    ordered_fields = [
        "shift_id",
        "start_ts",
        "end_ts",
        "start_time",
        "end_time",
        "code",
        "label",
        "description",
        "title",
        "notes",
        "comments",
        "tool",
        "tools_csv",
        "hole_id",
        "qaqc",
        "user_name",
        "created_at",
        "updated_at",
    ]
    payload = {
        "shift_id": sh["id"],
        "start_ts": start_val,
        "end_ts": end_val,
        "start_time": start_val,
        "end_time": end_val,
        "code": code_val,
        "label": label_val,
        "description": label_val,
        "title": label_val,
        "notes": a.get("notes"),
        "comments": a.get("notes"),
        "tool": a.get("tool"),
        "tools_csv": a.get("tool"),
        "hole_id": hole_id_val,
        "qaqc": None,
        "user_name": username,
        "created_at": ts,
        "updated_at": ts,
    }
    insert_cols = [c for c in ordered_fields if c in cols]
    if not insert_cols:
        conn.close()
        raise sqlite3.OperationalError("activities table has no recognized columns")

    placeholders = ",".join(["?"] * len(insert_cols))
    sql = f"INSERT INTO activities({','.join(insert_cols)}) VALUES({placeholders});"
    _retry_locked(lambda: conn.execute(sql, tuple(payload[c] for c in insert_cols)))
    conn.commit()
    conn.close()


def delete_activity(shift_date: str | date, username: str, activity_id: int) -> None:
    dt_str = shift_date.isoformat() if isinstance(shift_date, date) else str(shift_date)

    if backend() == "snowflake":
        s = _try_get_sf_session()
        assert s is not None
        s.sql(
            "DELETE FROM PUMA_ACTIVITIES WHERE ID=? AND SHIFT_DATE=TO_DATE(?) AND USERNAME=?;",
            params=[int(activity_id), dt_str, username],
        ).collect()
        return

    conn = _sqlite_conn()
    sh = _sqlite_get_shift(conn, dt_str, username)
    if not sh:
        conn.close()
        return
    _retry_locked(lambda: conn.execute("DELETE FROM activities WHERE id=? AND shift_id=?;", (int(activity_id), sh["id"])))
    conn.commit()
    conn.close()


def update_activity(shift_date: str | date, username: str, activity_id: int, a: Dict[str, Any]) -> None:
    start_val = a.get("start_ts") or a.get("start_time")
    end_val = a.get("end_ts") or a.get("end_time") or start_val
    code_val = a.get("code")
    label_val = a.get("label")
    hole_id_val = _normalize_hole_id(a.get("hole_id"))
    if code_val == "LOG" and not hole_id_val:
        hole_id_val = _generate_hole_id()

    for name, val in (("start_ts", start_val), ("end_ts", end_val), ("code", code_val), ("label", label_val)):
        if val is None or (isinstance(val, str) and not str(val).strip()):
            raise ValueError(f"Missing activity field: {name}")

    dt_str = shift_date.isoformat() if isinstance(shift_date, date) else str(shift_date)

    if backend() == "snowflake":
        s = _try_get_sf_session()
        assert s is not None
        _sf_ensure_hole(s, hole_id_val)
        s.sql(
            """
            UPDATE PUMA_ACTIVITIES
              SET START_TS=TO_TIMESTAMP_NTZ(?), END_TS=TO_TIMESTAMP_NTZ(?),
                  CODE=?, LABEL=?, NOTES=?, TOOL=?, HOLE_ID=?, UPDATED_AT=CURRENT_TIMESTAMP()
            WHERE ID=? AND SHIFT_DATE=TO_DATE(?) AND USERNAME=?;
            """,
            params=[start_val, end_val, code_val, label_val, a.get("notes"), a.get("tool"), hole_id_val, int(activity_id), dt_str, username],
        ).collect()
        return

    conn = _sqlite_conn()
    sh = _sqlite_get_shift(conn, dt_str, username)
    if not sh:
        conn.close()
        raise ValueError("Shift does not exist yet.")

    schema = _sqlite_activity_schema(conn)
    ts = _now()
    if hole_id_val:
        _sqlite_upsert_hole(conn, hole_id_val, ts)
    if schema == "new":
        _retry_locked(
            lambda: conn.execute(
                """
                UPDATE activities
                   SET start_ts=?, end_ts=?, code=?, label=?, notes=?, tool=?, hole_id=?, updated_at=?
                 WHERE id=? AND shift_id=?;
                """,
                (start_val, end_val, code_val, label_val, a.get("notes"), a.get("tool"), hole_id_val, ts, int(activity_id), sh["id"]),
            )
        )
    else:
        _retry_locked(
            lambda: conn.execute(
                """
                UPDATE activities
                   SET start_time=?, end_time=?, code=?, description=?, tools_csv=?, comments=?, hole_id=?, updated_at=?
                 WHERE id=? AND shift_id=?;
                """,
                (start_val, end_val, code_val, label_val, a.get("tool"), a.get("notes"), hole_id_val, ts, int(activity_id), sh["id"]),
            )
        )
    conn.commit()
    conn.close()
