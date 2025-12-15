import os, json, sqlite3
from datetime import datetime, date, time, timedelta
from typing import List, Dict, Any, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st

APP_TITLE = "Project Puma — Wireline Shift Activity Log (WSG)"
DB_PATH = os.path.join("data", "project_puma.db")
USERS_PATH = os.path.join("config", "users.json")
CATALOG_PATH = os.path.join("config", "catalog.json")

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

def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS shifts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      job_number TEXT NOT NULL,
      site_name TEXT,
      shift_date TEXT NOT NULL,
      shift_type TEXT NOT NULL,
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
    conn.commit()
    conn.close()

def load_catalog():
    users = _safe_load_json(USERS_PATH, {"users": ["Kyle"]}).get("users", ["Kyle"])
    cat = _safe_load_json(CATALOG_PATH, {})
    codes = cat.get("activity_codes", [{"code": "LOG", "label": "Logging"}])
    tools = cat.get("tools", [])
    return codes, tools, users

def list_shifts():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM shifts ORDER BY shift_date DESC, id DESC").fetchall()
    conn.close()
    return rows

def create_shift(job_number, site_name, shift_date, shift_type, shift_start, shift_hours, active_user) -> int:
    conn = get_conn()
    cur = conn.cursor()
    now = _now_iso()
    cur.execute("""
      INSERT INTO shifts (job_number, site_name, shift_date, shift_type, shift_start, shift_hours, active_user, synced, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
    """, (job_number, site_name, shift_date, shift_type, shift_start, shift_hours, active_user, now, now))
    conn.commit()
    sid = int(cur.lastrowid)
    conn.close()
    return sid

def update_shift_user(shift_id: int, active_user: str) -> None:
    conn = get_conn()
    conn.execute("UPDATE shifts SET active_user=?, updated_at=? WHERE id=?", (active_user, _now_iso(), shift_id))
    conn.commit()
    conn.close()

def get_shift(shift_id: int):
    conn = get_conn()
    row = conn.execute("SELECT * FROM shifts WHERE id=?", (shift_id,)).fetchone()
    conn.close()
    return row

def list_activities(shift_id: int):
    conn = get_conn()
    rows = conn.execute("SELECT * FROM activities WHERE shift_id=? ORDER BY start_time ASC, id ASC", (shift_id,)).fetchall()
    conn.close()
    return rows

def add_activity(shift_id: int, start_hhmm: str, end_hhmm: str, code: str, description: str,
                 tools: List[str], comments: str, qaqc: str, user_name: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    now = _now_iso()
    tools_csv = ",".join([t.strip() for t in tools if t.strip()]) if tools else ""
    cur.execute("""
      INSERT INTO activities (shift_id, start_time, end_time, code, description, tools_csv, comments, qaqc, user_name, created_at, updated_at)
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

def _overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return max(a_start, b_start) < min(a_end, b_end)

def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    init_db()
    codes, tools, users = load_catalog()
    code_options = [c["code"] for c in codes]
    code_labels = {c["code"]: c["label"] for c in codes}

    st.sidebar.markdown(f"## {APP_TITLE}")
    shifts = list_shifts()

    with st.sidebar.expander("➕ New Shift", expanded=(len(shifts) == 0)):
        c1, c2 = st.columns(2)
        job_number = c1.text_input("Job Number", value="25-00-XXX WSG")
        site_name = c2.text_input("Site / Client", value="WA Mine Site")
        c3, c4, c5 = st.columns(3)
        shift_date_val = c3.date_input("Shift Date", value=date.today())
        shift_type = c4.selectbox("Shift Type", ["Day", "Night", "Other"], index=0)
        active_user = c5.selectbox("Default User", users, index=0)
        c6, c7 = st.columns(2)
        shift_start = c6.time_input("Shift Start", value=time(6, 0))
        shift_hours = c7.number_input("Shift Hours", min_value=1.0, max_value=24.0, value=12.0, step=0.5)

        if st.button("Create Shift", use_container_width=True):
            sid = create_shift(
                job_number=job_number.strip() or "UNKNOWN",
                site_name=site_name.strip(),
                shift_date=shift_date_val.isoformat(),
                shift_type=shift_type,
                shift_start=_hhmm(shift_start),
                shift_hours=float(shift_hours),
                active_user=active_user,
            )
            st.session_state["shift_id"] = sid
            st.rerun()

    shifts = list_shifts()
    if not shifts:
        st.info("Create a shift in the sidebar to start.")
        return

    shift_labels = [f'#{s["id"]} | {s["shift_date"]} {s["shift_type"]} | {s["job_number"]}' for s in shifts]
    shift_ids = [int(s["id"]) for s in shifts]

    default_ix = 0
    if "shift_id" in st.session_state and st.session_state["shift_id"] in shift_ids:
        default_ix = shift_ids.index(st.session_state["shift_id"])

    pick = st.sidebar.selectbox("Load Shift", shift_labels, index=default_ix)
    shift_id = shift_ids[shift_labels.index(pick)]
    st.session_state["shift_id"] = shift_id

    shift = get_shift(shift_id)
    acts = list_activities(shift_id)

    left, right = st.columns([0.70, 0.30])
    with left:
        st.markdown(f"# {shift['job_number']}")
        st.caption(f"{shift['site_name'] or ''} • {shift['shift_date']} {shift['shift_type']} • Start {shift['shift_start']} • {shift['shift_hours']}h")
    with right:
        active_user = st.selectbox("Active User", users, index=users.index(shift["active_user"]) if shift["active_user"] in users else 0)
        if active_user != shift["active_user"]:
            update_shift_user(shift_id, active_user)
            shift = get_shift(shift_id)
        st.markdown(f"**Status:** {'✅ Synced' if shift['synced'] == 1 else '🟡 Local only'}")

    st.markdown("## Timeline")
    base_date = date.fromisoformat(shift["shift_date"])
    shift_start_dt = _combine_dt(base_date, _parse_hhmm(shift["shift_start"]))
    shift_end_dt = shift_start_dt + timedelta(hours=float(shift["shift_hours"]))

    if acts:
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
                "label": f'{r["code"]}: {r["description"]}',
                "user": r["user_name"],
                "tools": r["tools_csv"] or "",
                "qaqc": r["qaqc"] or "",
            })
        df_t = pd.DataFrame(rows)
        fig = px.timeline(df_t, x_start="start", x_end="end", y=["Shift"] * len(df_t), color="code",
                          hover_data=["id", "label", "user", "tools", "qaqc"])
        fig.update_yaxes(title=None, showticklabels=False)
        fig.update_xaxes(range=[shift_start_dt, shift_end_dt], title=None)
        fig.update_layout(height=220, margin=dict(l=20, r=20, t=10, b=10), legend_title_text="Code")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No activities yet. Add your first entry below.")

    st.markdown("## Add Activity")
    last_end = _parse_hhmm(acts[-1]["end_time"]) if acts else _parse_hhmm(shift["shift_start"])
    default_end = (_combine_dt(base_date, last_end) + timedelta(minutes=15)).time()

    with st.form("add_activity", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns([0.16, 0.16, 0.14, 0.20])
        start_t = c1.time_input("Start", value=last_end)
        end_t = c2.time_input("End", value=default_end)
        code = c3.selectbox("Code", code_options, index=code_options.index("LOG") if "LOG" in code_options else 0)
        user_name = c4.selectbox("User", users, index=users.index(shift["active_user"]) if shift["active_user"] in users else 0)
        st.caption(code_labels.get(code, ""))

        description = st.text_input("Description", placeholder="e.g. Pre-start checks / Drive to site / Gamma log Hole BH123 100m")
        tools_sel = st.multiselect("Tools Used (if applicable)", tools, default=[])
        c5, c6 = st.columns(2)
        comments = c5.text_area("Comments", height=90)
        qaqc = c6.text_area("QA/QC", height=90)

        allow_overlap = st.checkbox("Allow overlap (not recommended)", value=False)
        submitted = st.form_submit_button("Add Activity", use_container_width=True)

    if submitted:
        if not description.strip():
            st.error("Description is required.")
            st.stop()

        new_st_dt = _combine_dt(base_date, start_t)
        new_en_dt = _combine_dt(base_date, end_t)
        if new_en_dt <= new_st_dt:
            new_en_dt += timedelta(days=1)

        if not allow_overlap:
            for r in acts:
                st_dt = _combine_dt(base_date, _parse_hhmm(r["start_time"]))
                en_dt = _combine_dt(base_date, _parse_hhmm(r["end_time"]))
                if en_dt <= st_dt:
                    en_dt += timedelta(days=1)
                if _overlaps(new_st_dt, new_en_dt, st_dt, en_dt):
                    st.error("This entry overlaps an existing activity. Adjust times or tick 'Allow overlap'.")
                    st.stop()

        add_activity(
            shift_id=shift_id,
            start_hhmm=_hhmm(start_t),
            end_hhmm=_hhmm(end_t),
            code=code,
            description=description,
            tools=tools_sel,
            comments=comments,
            qaqc=qaqc,
            user_name=user_name,
        )
        st.success("Activity added.")
        st.rerun()

    st.markdown("## Activities")
    acts = list_activities(shift_id)
    if not acts:
        return

    df = pd.DataFrame([dict(r) for r in acts])
    st.dataframe(df, use_container_width=True, hide_index=True)

    with st.expander("🗑️ Delete an entry"):
        pick_id = st.selectbox("Activity ID", [int(r["id"]) for r in acts], index=0)
        if st.button("Delete", use_container_width=True):
            delete_activity(int(pick_id))
            st.warning("Deleted.")
            st.rerun()

if __name__ == "__main__":
    main()
