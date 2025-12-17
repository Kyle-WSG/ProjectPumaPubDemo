import os
import json
import sqlite3
import time
from datetime import datetime, date
from typing import Any, Dict, List, Optional

DB_PATH = os.path.join("data", "project_puma.db")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _try_get_sf_session():
    try:
        from snowflake.snowpark.context import get_active_session  # type: ignore
        return get_active_session()
    except Exception:
        return None


def backend() -> str:
    return "snowflake" if _try_get_sf_session() is not None else "sqlite"


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
        dupes = conn.execute(
            "SELECT shift_date, username, MIN(id) keep_id, GROUP_CONCAT(id) ids, COUNT(*) n "
            "FROM shifts GROUP BY shift_date, username HAVING n > 1;"
        ).fetchall()
        for r in dupes:
            keep_id = int(r[2])
            ids = [int(x) for x in str(r[3]).split(",") if x.strip().isdigit()]
            for sid in ids:
                if sid == keep_id:
                    continue
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
        or "start_time" in cols
        or "end_time" in cols
    )
    if not needs_migration:
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
              FOREIGN KEY(shift_id) REFERENCES shifts(id) ON DELETE CASCADE
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
        hole_id_val = d.get("hole_id")
        code_val = d.get("code") or "OTH"
        created_at = d.get("created_at") or now_ts
        updated_at = d.get("updated_at") or created_at
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
          FOREIGN KEY(shift_id) REFERENCES shifts(id) ON DELETE CASCADE
        );
        """
    )

    c.execute("CREATE INDEX IF NOT EXISTS idx_shifts_user_date ON shifts(username, shift_date);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_acts_shift_start ON activities(shift_id, start_ts);")

    # Add columns if the DB was created with an older schema
    cols = set(_sqlite_cols(c, "shifts"))

    def add_col(name: str, ddl: str) -> None:
        if name in cols:
            return
        _retry_locked(lambda: c.execute(ddl))
        cols.add(name)

    add_col("client", "ALTER TABLE shifts ADD COLUMN client TEXT NOT NULL DEFAULT 'Other';")
    add_col("site", "ALTER TABLE shifts ADD COLUMN site TEXT NOT NULL DEFAULT 'Other';")
    add_col("site_other", "ALTER TABLE shifts ADD COLUMN site_other TEXT;")
    add_col("job_number", "ALTER TABLE shifts ADD COLUMN job_number TEXT NOT NULL DEFAULT 'UNKNOWN';")
    add_col("vehicle_barcode", "ALTER TABLE shifts ADD COLUMN vehicle_barcode TEXT NOT NULL DEFAULT 'UNSET';")
    add_col("vehicle_name", "ALTER TABLE shifts ADD COLUMN vehicle_name TEXT NOT NULL DEFAULT 'UNSET';")
    add_col("vehicle_description", "ALTER TABLE shifts ADD COLUMN vehicle_description TEXT;")
    add_col("vehicle_model", "ALTER TABLE shifts ADD COLUMN vehicle_model TEXT;")
    add_col("vehicle_category", "ALTER TABLE shifts ADD COLUMN vehicle_category TEXT;")
    add_col("vehicle_location_expected", "ALTER TABLE shifts ADD COLUMN vehicle_location_expected TEXT;")
    add_col("vehicle_location_actual", "ALTER TABLE shifts ADD COLUMN vehicle_location_actual TEXT;")
    add_col("vehicle_location_mismatch", "ALTER TABLE shifts ADD COLUMN vehicle_location_mismatch INTEGER NOT NULL DEFAULT 0;")
    add_col("shift_hours", "ALTER TABLE shifts ADD COLUMN shift_hours REAL NOT NULL DEFAULT 12;")
    add_col("shift_notes", "ALTER TABLE shifts ADD COLUMN shift_notes TEXT;")

    # Add activity columns that may be missing in older DBs
    act_cols = set(_sqlite_cols(c, "activities"))
    if "hole_id" not in act_cols:
        _retry_locked(lambda: c.execute("ALTER TABLE activities ADD COLUMN hole_id TEXT;"))
        act_cols.add("hole_id")

    _sqlite_dedupe_shifts(c)
    _sqlite_migrate_activities_to_new(c)
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
    row = conn.execute(
        "SELECT * FROM shifts WHERE shift_date=? AND username=? ORDER BY updated_at DESC, id DESC LIMIT 1;",
        (shift_date, username),
    ).fetchone()
    if row is None:
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

    if existing:
        sets = ",".join([f"{k}=?" for k in payload.keys()])
        _retry_locked(
            lambda: conn.execute(
                f"UPDATE shifts SET {sets} WHERE shift_date=? AND username=?;",
                tuple(payload.values()) + (dt_str, d.get("username")),
            )
        )
    else:
        payload["created_at"] = ts
        cols = ",".join(payload.keys())
        vals = ":" + ",:".join(payload.keys())
        _retry_locked(lambda: conn.execute(f"INSERT INTO shifts({cols}) VALUES({vals});", payload))

    conn.commit()
    out = _sqlite_get_shift(conn, dt_str, d.get("username"))
    conn.close()
    assert out is not None
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

    for name, val in (("start_ts", start_val), ("end_ts", end_val), ("code", code_val), ("label", label_val)):
        if val is None or (isinstance(val, str) and not str(val).strip()):
            raise ValueError(f"Missing activity field: {name}")

    dt_str = shift_date.isoformat() if isinstance(shift_date, date) else str(shift_date)

    if backend() == "snowflake":
        s = _try_get_sf_session()
        assert s is not None
        s.sql(
            """
            INSERT INTO PUMA_ACTIVITIES(
              SHIFT_DATE, USERNAME, START_TS, END_TS, CODE, LABEL, NOTES, TOOL, HOLE_ID, CREATED_AT, UPDATED_AT
            ) VALUES(TO_DATE(?), ?, TO_TIMESTAMP_NTZ(?), TO_TIMESTAMP_NTZ(?), ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
            """,
            params=[dt_str, username, start_val, end_val, code_val, label_val, a.get("notes"), a.get("tool"), a.get("hole_id")],
        ).collect()
        return

    conn = _sqlite_conn()
    sh = _sqlite_get_shift(conn, dt_str, username)
    if not sh:
        conn.close()
        raise ValueError("Shift does not exist yet.")

    ts = _now()
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
        "hole_id": a.get("hole_id"),
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

    for name, val in (("start_ts", start_val), ("end_ts", end_val), ("code", code_val), ("label", label_val)):
        if val is None or (isinstance(val, str) and not str(val).strip()):
            raise ValueError(f"Missing activity field: {name}")

    dt_str = shift_date.isoformat() if isinstance(shift_date, date) else str(shift_date)

    if backend() == "snowflake":
        s = _try_get_sf_session()
        assert s is not None
        s.sql(
            """
            UPDATE PUMA_ACTIVITIES
              SET START_TS=TO_TIMESTAMP_NTZ(?), END_TS=TO_TIMESTAMP_NTZ(?),
                  CODE=?, LABEL=?, NOTES=?, TOOL=?, HOLE_ID=?, UPDATED_AT=CURRENT_TIMESTAMP()
            WHERE ID=? AND SHIFT_DATE=TO_DATE(?) AND USERNAME=?;
            """,
            params=[start_val, end_val, code_val, label_val, a.get("notes"), a.get("tool"), a.get("hole_id"), int(activity_id), dt_str, username],
        ).collect()
        return

    conn = _sqlite_conn()
    sh = _sqlite_get_shift(conn, dt_str, username)
    if not sh:
        conn.close()
        raise ValueError("Shift does not exist yet.")

    schema = _sqlite_activity_schema(conn)
    ts = _now()
    if schema == "new":
        _retry_locked(
            lambda: conn.execute(
                """
                UPDATE activities
                   SET start_ts=?, end_ts=?, code=?, label=?, notes=?, tool=?, hole_id=?, updated_at=?
                 WHERE id=? AND shift_id=?;
                """,
                (start_val, end_val, code_val, label_val, a.get("notes"), a.get("tool"), a.get("hole_id"), ts, int(activity_id), sh["id"]),
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
                (start_val, end_val, code_val, label_val, a.get("tool"), a.get("notes"), a.get("hole_id"), ts, int(activity_id), sh["id"]),
            )
        )
    conn.commit()
    conn.close()
