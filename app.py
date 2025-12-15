import os, json, sqlite3
from datetime import datetime, date, time, timedelta
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st

APP_TITLE = "Project Puma — Shift Activity Log"
DB_PATH = os.path.join("data", "project_puma.db")
USERS_PATH = os.path.join("config", "users.json")
VEHICLES_PATH = os.path.join("config", "vehicles.json")
CATALOG_PATH = os.path.join("config", "catalog.json")

TOOL_RELEVANT_CODES = {"LOG", "CAL", "QAQ", "NPT"}  # show tools only when relevant

# ----------------- helpers -----------------
def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")

def _hhmm(t: time) -> str:
    return f"{t.hour:02d}:{t.minute:02d}"

def _parse_hhmm(s: str) -> time:
    h, m = s.split(":")
    return time(int(h), int(m))

def _combine_dt(d: date, t: time) -> datetime:
    return datetime(d.year, d.month, d.day, t.hour, t.minute, 0)

def _safe_load_json(path: str, fallback: Dict[str, Any]) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return fallback

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def _col_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cols = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return any(r["name"] == col for r in cols)

# ----------------- db -----------------
def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS shifts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      shift_date TEXT NOT NULL,
      shift_type TEXT NOT NULL,
      vehicle TEXT NOT NULL,
      job_number TEXT NOT NULL,
      site_name TEXT,
      shift_start TEXT NOT NULL,
      shift_hours REAL NOT NULL,
      active_user TEXT NOT NULL,
      synced INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS activities (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      shift_id INTEGER NOT NULL,
      start_time TEXT NOT NULL,
      end_time TEXT NOT NULL,
      code TEXT NOT NULL,
      description TEXT NOT NULL,
      tools_csv TEXT,
      comments TEXT,
      qaqc TEXT,
      user_name TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY (shift_id) REFERENCES shifts(id)
    );
    """)

    # Index for quick lookup by key
    # --- migrations for older DBs ---
    if not _col_exists(conn, "shifts", "vehicle"):
        cur.execute("ALTER TABLE shifts ADD COLUMN vehicle TEXT NOT NULL DEFAULT 'UNSET';")
    
    cur.execute("CREATE INDEX IF NOT EXISTS idx_shifts_key ON shifts(shift_date, shift_type, vehicle);")
    conn.commit()
    conn.close()

def get_shift_by_key(shift_date: str, shift_type: str, vehicle: str) -> Optional[sqlite3.Row]:
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM shifts WHERE shift_date=? AND shift_type=? AND vehicle=? ORDER BY id DESC LIMIT 1",
        (shift_date, shift_type, vehicle),
    ).fetchone()
    conn.close()
    return row

def create_shift(
    shift_date: str, shift_type: str, vehicle: str,
    job_number: str, site_name: str, shift_start: str, shift_hours: float,
    active_user: str
) -> int:
    conn = get_conn()
    cur = conn.cursor()
    now = _now_iso()
    cur.execute("""
      INSERT INTO shifts
      (shift_date, shift_type, vehicle, job_number, site_name, shift_start, shift_hours, active_user, synced, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
    """, (shift_date, shift_type, vehicle, job_number, site_name, shift_start, float(shift_hours), active_user, now, now))
    conn.commit()
    sid = int(cur.lastrowid)
    conn.close()
    return sid

def update_shift_details(shift_id: int, job_number: str, site_name: str, shift_start: str, shift_hours: float, active_user: str) -> None:
    conn = get_conn()
    conn.execute("""
      UPDATE shifts
      SET job_number=?, site_name=?, shift_start=?, shift_hours=?, active_user=?, updated_at=?
      WHERE id=?
    """, (job_number, site_name, shift_start, float(shift_hours), active_user, _now_iso(), shift_id))
    conn.commit()
    conn.close()

def list_activities(shift_id: int) -> List[sqlite3.Row]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM activities WHERE shift_id=? ORDER BY start_time ASC, id ASC",
        (shift_id,),
    ).fetchall()
    conn.close()
    return rows

def add_activity(
    shift_id: int, start_hhmm: str, end_hhmm: str, code: str, description: str,
    tools: List[str], comments: str, qaqc: str, user_name: str
) -> int:
    conn = get_conn()
    cur = conn.cursor()
    now = _now_iso()
    tools_csv = ",".join([t.strip() for t in tools if t.strip()]) if tools else ""
    cur.execute("""
      INSERT INTO activities
      (shift_id, start_time, end_time, code, description, tools_csv, comments, qaqc, user_name, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (shift_id, start_hhmm, end_hhmm, code, description.strip(), tools_csv, comments.strip(), qaqc.strip(), user_name, now, now))
    conn.commit()
    aid = int(cur.lastrowid)
    conn.close()
    return aid

def delete_activity(activity_id: int) -> None:
    conn = get_conn()
    conn.execute("DELETE FROM activities WHERE id=?", (activity_id,))
    conn.commit()
    conn.close()

# ----------------- logic -----------------
def load_config():
    users = _safe_load_json(USERS_PATH, {"users": ["Kyle"]}).get("users", ["Kyle"])
    vehicles = _safe_load_json(VEHICLES_PATH, {"vehicles": []}).get("vehicles", [])
    cat = _safe_load_json(CATALOG_PATH, {})
    codes = cat.get("activity_codes", [{"code": "LOG", "label": "Logging Run / On-Tool Ops"}])
    tools = cat.get("tools", [])
    return users, vehicles, codes, tools

def _duration_minutes(d: date, start_hhmm: str, end_hhmm: str) -> int:
    st_dt = _combine_dt(d, _parse_hhmm(start_hhmm))
    en_dt = _combine_dt(d, _parse_hhmm(end_hhmm))
    if en_dt <= st_dt:
        en_dt += timedelta(days=1)
    return int((en_dt - st_dt).total_seconds() // 60)

def _overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return max(a_start, b_start) < min(a_end, b_end)

def ensure_session_defaults(users: List[str], vehicles: List[str]) -> None:
    st.session_state.setdefault("view", "home")  # home | add
    st.session_state.setdefault("active_date", date.today())
    st.session_state.setdefault("shift_type", "Day")
    st.session_state.setdefault("active_user", users[0] if users else "User")
    st.session_state.setdefault("vehicle", vehicles[0] if vehicles else "")
    st.session_state.setdefault("job_number", "")
    st.session_state.setdefault("site_name", "")
    st.session_state.setdefault("shift_start", time(6, 0))
    st.session_state.setdefault("shift_hours", 12.0)

def get_or_create_shift_for_screen(active_date: date, shift_type: str, vehicle: str, active_user: str) -> Optional[int]:
    # Vehicle is mandatory (per your requirement)
    if not vehicle or vehicle.strip() == "":
        return None

    shift_date_str = active_date.isoformat()
    existing = get_shift_by_key(shift_date_str, shift_type, vehicle)

    # Choose defaults based on shift type
    default_start = time(6, 0) if shift_type == "Day" else time(18, 0)

    if existing:
        # keep app fields synced from DB
        st.session_state["job_number"] = existing["job_number"]
        st.session_state["site_name"] = existing["site_name"] or ""
        st.session_state["shift_start"] = _parse_hhmm(existing["shift_start"])
        st.session_state["shift_hours"] = float(existing["shift_hours"])
        return int(existing["id"])

    # Create brand new shift with sensible defaults (job_number required before adding activity)
    job = st.session_state.get("job_number", "").strip() or "UNSET"
    site = st.session_state.get("site_name", "").strip()
    sid = create_shift(
        shift_date=shift_date_str,
        shift_type=shift_type,
        vehicle=vehicle.strip(),
        job_number=job,
        site_name=site,
        shift_start=_hhmm(default_start),
        shift_hours=12.0,
        active_user=active_user,
    )
    st.session_state["shift_start"] = default_start
    st.session_state["shift_hours"] = 12.0
    return sid

# ----------------- UI -----------------
def hide_streamlit_sidebar():
    st.markdown("""
    <style>
      [data-testid="stSidebar"] { display: none; }
      [data-testid="collapsedControl"] { display: none; }
      .block-container { padding-top: 0.8rem; }
      .puma-topbar {
        position: sticky; top: 0; z-index: 999;
        background: rgba(22,27,34,0.92);
        border: 1px solid rgba(255,255,255,0.10);
        border-radius: 14px;
        padding: 0.55rem 0.75rem;
        backdrop-filter: blur(8px);
        margin-bottom: 0.75rem;
      }
      .puma-subtle { color: rgba(230,237,243,0.75); }
      .puma-card {
        background: rgba(22,27,34,0.60);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px;
        padding: 0.75rem;
      }
      div.stButton>button { border-radius: 12px; padding: 0.65rem 1rem; font-weight: 700; }
    </style>
    """, unsafe_allow_html=True)

def render_code_help(codes: List[Dict[str, str]]):
    with st.expander("ℹ️ Activity Code Help (what each code means)", expanded=False):
        for c in codes:
            st.markdown(f"**{c['code']}** — {c.get('label','')}")
        st.caption("Tools Used only appears for LOG/CAL/QAQ/NPT entries (tool-relevant work).")

def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    hide_streamlit_sidebar()
    init_db()

    users, vehicles, codes, tools = load_config()
    ensure_session_defaults(users, vehicles)
    code_options = [c["code"] for c in codes]
    code_labels = {c["code"]: c.get("label", "") for c in codes}

    # --- TOP BAR (User + Vehicle + Shift Type) ---
    st.markdown('<div class="puma-topbar">', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns([0.24, 0.26, 0.18, 0.32])
    st.session_state["active_user"] = c1.selectbox("User", users, index=users.index(st.session_state["active_user"]) if st.session_state["active_user"] in users else 0)
    st.session_state["vehicle"] = c2.selectbox("Vehicle (required)", vehicles if vehicles else ["(add vehicles in config/vehicles.json)"], index=0)
    st.session_state["shift_type"] = c3.selectbox("Shift", ["Day", "Night", "Other"], index=["Day","Night","Other"].index(st.session_state["shift_type"]) if st.session_state["shift_type"] in ["Day","Night","Other"] else 0)

    status_text = "🟡 Local only (SQLite)"
    c4.markdown(f"**Status:** {status_text}")
    st.markdown("</div>", unsafe_allow_html=True)

    # --- SECOND HEADER: Day/Shift + Date nav + Add Activity ---
    nav1, nav2, nav3, nav4, nav5 = st.columns([0.10, 0.10, 0.40, 0.20, 0.20])

    if nav1.button("◀ Prev", use_container_width=True):
        st.session_state["active_date"] = st.session_state["active_date"] - timedelta(days=1)
        st.session_state["view"] = "home"
        st.rerun()

    if nav2.button("Today", use_container_width=True):
        st.session_state["active_date"] = date.today()
        st.session_state["view"] = "home"
        st.rerun()

    pretty = st.session_state["active_date"].strftime("%a %d %b %Y")
    nav3.markdown(f"## {st.session_state['shift_type']} Shift — {pretty}")

    if nav4.button("Next ▶", use_container_width=True):
        st.session_state["active_date"] = st.session_state["active_date"] + timedelta(days=1)
        st.session_state["view"] = "home"
        st.rerun()

    # vehicle gate for Add Activity
    can_add = bool(st.session_state["vehicle"] and st.session_state["vehicle"].strip())
    if nav5.button("➕ Add Activity", use_container_width=True, disabled=not can_add):
        st.session_state["view"] = "add"
        st.rerun()

    if not can_add:
        st.warning("Select a **Vehicle** in the top bar to start the shift log.")

    # --- Shift get/create (by date+shift+vehicle) ---
    shift_id = get_or_create_shift_for_screen(
        active_date=st.session_state["active_date"],
        shift_type=st.session_state["shift_type"],
        vehicle=st.session_state["vehicle"],
        active_user=st.session_state["active_user"],
    )

    if shift_id is None:
        render_code_help(codes)
        st.stop()

    # --- Shift Setup (on-screen, no sidebar) ---
    with st.expander("⚙️ Shift Setup (Job, Site, Start Time, Hours) — required before logging", expanded=False):
        s1, s2, s3, s4 = st.columns([0.25, 0.25, 0.25, 0.25])
        job_number = s1.text_input("Job Number (required)", value=st.session_state.get("job_number",""))
        site_name = s2.text_input("Site / Client", value=st.session_state.get("site_name",""))
        shift_start = s3.time_input("Shift Start", value=st.session_state.get("shift_start", time(6,0)))
        shift_hours = s4.number_input("Shift Hours", min_value=1.0, max_value=24.0, value=float(st.session_state.get("shift_hours", 12.0)), step=0.5)

        if st.button("Save Shift Setup", use_container_width=True):
            if not job_number.strip():
                st.error("Job Number is required before you start logging.")
                st.stop()
            update_shift_details(
                shift_id=shift_id,
                job_number=job_number.strip(),
                site_name=site_name.strip(),
                shift_start=_hhmm(shift_start),
                shift_hours=float(shift_hours),
                active_user=st.session_state["active_user"],
            )
            st.session_state["job_number"] = job_number.strip()
            st.session_state["site_name"] = site_name.strip()
            st.session_state["shift_start"] = shift_start
            st.session_state["shift_hours"] = float(shift_hours)
            st.success("Saved.")
            st.rerun()

    # hard gate: must have Job Number set before adding activities
    if st.session_state.get("job_number", "").strip() in ["", "UNSET"]:
        st.info("Set **Job Number** in **Shift Setup** before adding activities.")
        render_code_help(codes)
        if st.session_state["view"] == "add":
            st.session_state["view"] = "home"
        # still allow browsing existing entries if any
    # load activities
    acts = list_activities(shift_id)
    base_date = st.session_state["active_date"]
    shift_start_dt = _combine_dt(base_date, st.session_state.get("shift_start", time(6, 0)))
    shift_end_dt = shift_start_dt + timedelta(hours=float(st.session_state.get("shift_hours", 12.0)))

    # ----------------- ADD VIEW -----------------
    if st.session_state["view"] == "add":
        top = st.columns([0.15, 0.85])
        if top[0].button("⬅ Back", use_container_width=True):
            st.session_state["view"] = "home"
            st.rerun()

        st.markdown("### Add Activity")
        st.markdown(f"<div class='puma-subtle'>Vehicle: <b>{st.session_state['vehicle']}</b> • Job: <b>{st.session_state.get('job_number','')}</b></div>", unsafe_allow_html=True)

        render_code_help(codes)

        last_end = _parse_hhmm(acts[-1]["end_time"]) if acts else _parse_hhmm(_hhmm(st.session_state.get("shift_start", time(6, 0))))
        default_end = (_combine_dt(base_date, last_end) + timedelta(minutes=15)).time()

        with st.form("add_activity_form", clear_on_submit=True):
            a1, a2, a3, a4 = st.columns([0.16, 0.16, 0.16, 0.22])
            start_t = a1.time_input("Start", value=last_end)
            end_t = a2.time_input("End", value=default_end)
            code = a3.selectbox("Code", code_options, index=code_options.index("LOG") if "LOG" in code_options else 0)
            entry_user = a4.selectbox("User for this entry", users, index=users.index(st.session_state["active_user"]) if st.session_state["active_user"] in users else 0)

            st.caption(code_labels.get(code, ""))

            description = st.text_input("Description (required)", placeholder="e.g. Pre-start checks / Drive to site / Gamma log Hole BH123 100m")

            show_tools = code in TOOL_RELEVANT_CODES
            tools_sel: List[str] = []
            if show_tools:
                tools_sel = st.multiselect("Tools Used", tools, default=[])
            else:
                st.markdown("<div class='puma-subtle'>Tools Used hidden for this code (not tool-related).</div>", unsafe_allow_html=True)

            b1, b2 = st.columns(2)
            comments = b1.text_area("Comments", height=90)
            qaqc = b2.text_area("QA/QC", height=90)

            allow_overlap = st.checkbox("Allow overlap (not recommended)", value=False)
            submitted = st.form_submit_button("Save Activity", use_container_width=True)

        if submitted:
            if not st.session_state.get("job_number","").strip() or st.session_state.get("job_number","").strip() == "UNSET":
                st.error("Set Job Number first (Shift Setup).")
                st.stop()
            if not description.strip():
                st.error("Description is required.")
                st.stop()

            new_st_dt = _combine_dt(base_date, start_t)
            new_en_dt = _combine_dt(base_date, end_t)
            if new_en_dt <= new_st_dt:
                new_en_dt += timedelta(days=1)

            if new_st_dt < shift_start_dt or new_en_dt > shift_end_dt:
                st.warning("This entry is outside the shift window. Fix if unintended.")

            if not allow_overlap and acts:
                for r in acts:
                    st_dt = _combine_dt(base_date, _parse_hhmm(r["start_time"]))
                    en_dt = _combine_dt(base_date, _parse_hhmm(r["end_time"]))
                    if en_dt <= st_dt:
                        en_dt += timedelta(days=1)
                    if _overlaps(new_st_dt, new_en_dt, st_dt, en_dt):
                        st.error("Overlaps an existing activity. Adjust times or tick Allow overlap.")
                        st.stop()

            add_activity(
                shift_id=shift_id,
                start_hhmm=_hhmm(start_t),
                end_hhmm=_hhmm(end_t),
                code=code,
                description=description,
                tools=tools_sel if show_tools else [],
                comments=comments,
                qaqc=qaqc,
                user_name=entry_user,
            )
            st.success("Saved activity.")
            st.session_state["view"] = "home"
            st.rerun()

        st.stop()

    # ----------------- HOME VIEW -----------------
    st.markdown("### Shift Overview")
    st.markdown(
        f"<div class='puma-card'>"
        f"<b>Vehicle:</b> {st.session_state['vehicle']} &nbsp; | &nbsp; "
        f"<b>Job:</b> {st.session_state.get('job_number','')} &nbsp; | &nbsp; "
        f"<b>Site:</b> {st.session_state.get('site_name','')} &nbsp; | &nbsp; "
        f"<b>Window:</b> {shift_start_dt.strftime('%H:%M')} → {shift_end_dt.strftime('%H:%M')}"
        f"</div>",
        unsafe_allow_html=True,
    )

    # quick metrics
    total_logged_min = 0
    for r in acts:
        total_logged_min += _duration_minutes(base_date, r["start_time"], r["end_time"])
    shift_total_min = int((shift_end_dt - shift_start_dt).total_seconds() // 60)
    remaining_min = max(0, shift_total_min - total_logged_min)

    m1, m2, m3 = st.columns(3)
    m1.metric("Logged", f"{total_logged_min/60:.2f} h")
    m2.metric("Remaining", f"{remaining_min/60:.2f} h")
    m3.metric("Entries", f"{len(acts)}")

    # timeline
    st.markdown("## Timeline")
    if not acts:
        st.info("No activities yet. Hit **Add Activity** to start logging.")
    else:
        rows = []
        for r in acts:
            st_dt = _combine_dt(base_date, _parse_hhmm(r["start_time"]))
            en_dt = _combine_dt(base_date, _parse_hhmm(r["end_time"]))
            if en_dt <= st_dt:
                en_dt += timedelta(days=1)
            rows.append({
                "id": int(r["id"]),
                "start": st_dt,
                "end": en_dt,
                "code": r["code"],
                "desc": r["description"],
                "user": r["user_name"],
                "tools": r["tools_csv"] or "",
                "qaqc": r["qaqc"] or "",
            })
        df_t = pd.DataFrame(rows)
        fig = px.timeline(
            df_t,
            x_start="start",
            x_end="end",
            y=["Shift"] * len(df_t),
            color="code",
            hover_data=["id", "desc", "user", "tools", "qaqc"],
        )
        fig.update_yaxes(title=None, showticklabels=False)
        fig.update_xaxes(range=[shift_start_dt, shift_end_dt], title=None)
        fig.update_layout(height=240, margin=dict(l=20, r=20, t=10, b=10), legend_title_text="Code")
        st.plotly_chart(fig, use_container_width=True)

    # table
    st.markdown("## Activities")
    if acts:
        table = []
        for r in acts:
            table.append({
                "id": int(r["id"]),
                "start": r["start_time"],
                "end": r["end_time"],
                "code": r["code"],
                "description": r["description"],
                "tools": r["tools_csv"] or "",
                "user": r["user_name"],
                "qaqc": r["qaqc"] or "",
                "comments": r["comments"] or "",
            })
        df = pd.DataFrame(table)
        st.dataframe(df, use_container_width=True, hide_index=True)

        with st.expander("🗑️ Delete an entry"):
            pick_id = st.selectbox("Activity ID", df["id"].tolist(), index=0)
            if st.button("Delete", use_container_width=True):
                delete_activity(int(pick_id))
                st.warning("Deleted.")
                st.rerun()

    render_code_help(codes)

if __name__ == "__main__":
    main()
