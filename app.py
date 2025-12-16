import json
from datetime import datetime, timedelta, date
from pathlib import Path

import pandas as pd
import streamlit as st

import storage

APP_TITLE = "Project Puma — Shift Activity Log"

CODES = {
    "MOB": ("Mobilisation / Travel", "Driving to/from site, pattern runs, pickups."),
    "INSP": ("Pre-start / Inspection", "Pre-start checks, inspections, tool checks."),
    "SAF": ("Safety", "Take 5, JHA/JSA, safety meeting, risk controls."),
    "LOG": ("Logging", "Wireline logging ops (downhole / acquisition / QAQC)."),
    "CAL": ("Calibration", "Tool calibration, checks, source handling."),
    "MTG": ("Meeting", "Client meeting, toolbox talk, shift handover."),
    "ADM": ("Admin", "Paperwork, reporting, emails, system admin."),
    "DWN": ("Downtime / Waiting", "Waiting on drill, access, weather, breakdowns."),
}
LOGGING_CODES = {"LOG", "CAL"}  # only show tool field for these

def load_json_list(path: str, fallback):
    p = Path(path)
    if not p.exists():
        return fallback
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        # Allow either a bare list or a dict wrapper like {"vehicles": [...]}.
        if isinstance(data, dict):
            for key in ("vehicles", "items", "values", "data"):
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
        if isinstance(data, list):
            return data
        return fallback
    except Exception:
        return fallback

def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")

def init_state():
    st.session_state.setdefault("page", "login")  # login | dd
    st.session_state.setdefault("username", None)
    st.session_state.setdefault("shift_date", date.today())
    st.session_state.setdefault("shift_type", "Day")
    st.session_state.setdefault("dd_view", "list")  # list | add | edit_shift

def css():
    st.markdown("""
    <style>
      .card {border:1px solid rgba(255,255,255,0.08); border-radius:16px; padding:16px;
             background: rgba(255,255,255,0.02);}
      .muted {opacity:0.75;}
      .toprow {display:flex; align-items:center; gap:12px;}
      .toprow .spacer {flex:1;}
      .title {font-weight:800; font-size:1.15rem;}
    </style>
    """, unsafe_allow_html=True)

def codes_help():
    with st.expander("Code help / What do the activity codes mean?", expanded=False):
        rows = [{"Code": k, "Meaning": v[0], "When to use": v[1]} for k, v in CODES.items()]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

def login_page(users):
    st.markdown(f"<div class='card'><div class='title'>{APP_TITLE}</div><div class='muted'>Select your user to start (temporary login).</div></div>", unsafe_allow_html=True)
    st.write("")
    with st.container(border=True):
        user = st.selectbox("User", users, index=0)
        c1, c2 = st.columns([1, 2], vertical_alignment="center")
        with c2:
            if st.button("Enter", type="primary", use_container_width=True):
                st.session_state["username"] = user
                st.session_state["page"] = "dd"
                st.session_state["shift_date"] = date.today()
                st.session_state["shift_type"] = "Day"
                st.session_state["dd_view"] = "list"
                st.rerun()

def dd_header():
    # Top row: user + actions
    st.markdown("<div class='toprow'>"
                f"<div class='title'>Daily Diary</div>"
                f"<div class='muted'>User: <b>{st.session_state['username']}</b></div>"
                "<div class='spacer'></div>"
                "</div>", unsafe_allow_html=True)

    c1, c2, c3 = st.columns([6, 2, 2], vertical_alignment="center")
    with c2:
        if st.button("Edit shift details", use_container_width=True):
            st.session_state["dd_view"] = "edit_shift"
            st.rerun()
    with c3:
        if st.button("Switch user", use_container_width=True):
            st.session_state["page"] = "login"
            st.session_state["username"] = None
            st.session_state["dd_view"] = "list"
            st.rerun()

    # Date row: prev / today / next + day/night
    d1, d2, d3, d4, d5 = st.columns([1, 3, 2, 2, 1], vertical_alignment="center")
    with d1:
        if st.button("◀", use_container_width=True):
            st.session_state["shift_date"] = st.session_state["shift_date"] - timedelta(days=1)
            st.session_state["dd_view"] = "list"
            st.rerun()
    with d2:
        st.markdown(f"### {st.session_state['shift_date'].strftime('%a %d %b %Y')}")
    with d3:
        st.session_state["shift_type"] = st.segmented_control(
            "Shift",
            options=["Day", "Night"],
            default=st.session_state["shift_type"],
            label_visibility="collapsed",
        )
    with d4:
        if st.button("Today", use_container_width=True):
            st.session_state["shift_date"] = date.today()
            st.session_state["dd_view"] = "list"
            st.rerun()
    with d5:
        if st.button("▶", use_container_width=True):
            st.session_state["shift_date"] = st.session_state["shift_date"] + timedelta(days=1)
            st.session_state["dd_view"] = "list"
            st.rerun()

def shift_form(vehicles, existing=None):
    # existing may be dict or None
    defaults = {
        "vehicle": "UNSET",
        "job_number": "",
        "site_name": "",
        "shift_start": "",
        "shift_hours": 12.0,
        "shift_notes": "",
    }
    if existing:
        defaults.update({
            "vehicle": existing.get("vehicle") or "UNSET",
            "job_number": existing.get("job_number") or "",
            "site_name": existing.get("site_name") or "",
            "shift_start": existing.get("shift_start") or "",
            "shift_hours": float(existing.get("shift_hours") or 12.0),
            "shift_notes": existing.get("shift_notes") or "",
        })

    st.markdown("<div class='card'><div class='title'>Shift details</div><div class='muted'>Vehicle is required. If this shift doesn’t exist yet, saving will create it.</div></div>", unsafe_allow_html=True)
    st.write("")
    with st.form("shift_details"):
        c1, c2 = st.columns([1, 1])
        with c1:
            vehicle = st.selectbox("Vehicle (required)", vehicles, index=vehicles.index(defaults["vehicle"]) if defaults["vehicle"] in vehicles else 0)
        with c2:
            shift_start = st.text_input("Shift start (optional)", value=defaults["shift_start"], placeholder="e.g. 06:00 or 18:00")
        c3, c4, c5 = st.columns([1, 1, 2])
        with c3:
            job_number = st.text_input("Job number", value=defaults["job_number"])
        with c4:
            shift_hours = st.number_input("Shift hours", min_value=1.0, max_value=24.0, value=float(defaults["shift_hours"]), step=0.5)
        with c5:
            site_name = st.text_input("Site / Client", value=defaults["site_name"])

        shift_notes = st.text_area("Shift notes (optional)", value=defaults["shift_notes"])

        save = st.form_submit_button("Save shift details", type="primary", use_container_width=True)
        if save:
            if vehicle in ("", "UNSET", None):
                st.error("Vehicle is required.")
                st.stop()
            sid = storage.upsert_shift(
                shift_date=st.session_state["shift_date"],
                shift_type=st.session_state["shift_type"],
                username=st.session_state["username"],
                vehicle=vehicle,
                job_number=job_number,
                site_name=site_name,
                shift_start=shift_start,
                shift_hours=float(shift_hours),
                shift_notes=shift_notes,
            )
            st.success("Shift details saved.")
            st.session_state["dd_view"] = "list"
            st.rerun()

def activities_list(shift_id: int):
    st.divider()
    c1, c2 = st.columns([4, 1], vertical_alignment="center")
    with c1:
        st.caption(f"Storage backend: **{storage.backend()}**")
    with c2:
        if st.button("➕ Add activity", use_container_width=True):
            st.session_state["dd_view"] = "add"
            st.rerun()

    acts = storage.list_activities(shift_id)
    if not acts:
        st.info("No activities yet. Click **Add activity** to start.")
        return

    df = pd.DataFrame(acts)

    # Normalize columns
    ren = {
        "start_ts": "Start",
        "end_ts": "End",
        "code": "Code",
        "title": "Activity",
        "notes": "Notes",
        "tool_ref": "Tool",
        "START_TS": "Start",
        "END_TS": "End",
        "CODE": "Code",
        "TITLE": "Activity",
        "NOTES": "Notes",
        "TOOL_REF": "Tool",
        "id": "ID",
        "ID": "ID",
    }
    df = df.rename(columns=ren)
    cols = [c for c in ["ID", "Start", "End", "Code", "Activity", "Tool", "Notes"] if c in df.columns]
    st.dataframe(df[cols], use_container_width=True, hide_index=True)

    st.markdown("#### Delete an activity")
    del_id = st.number_input("Activity ID", min_value=0, step=1, value=0)
    if st.button("Delete selected ID", type="secondary"):
        if del_id > 0:
            storage.delete_activity(int(del_id))
            st.success("Deleted.")
            st.rerun()

def add_activity_view(shift_id: int):
    st.divider()
    c1, c2 = st.columns([1, 3], vertical_alignment="center")
    with c1:
        if st.button("← Back", use_container_width=True):
            st.session_state["dd_view"] = "list"
            st.rerun()
    with c2:
        st.caption("Add one activity entry")

    default_start = datetime.combine(st.session_state["shift_date"], datetime.now().time()).replace(second=0, microsecond=0)

    with st.form("add_activity", clear_on_submit=True):
        a1, a2 = st.columns([1, 1])
        with a1:
            start_dt = st.datetime_input("Start", value=default_start)
        with a2:
            end_dt = st.datetime_input("End (optional)", value=None)

        code = st.selectbox("Code", list(CODES.keys()))
        title = st.text_input("Activity", value=CODES[code][0])
        notes = st.text_area("Notes (optional)", value="")

        tool_ref = ""
        if code in LOGGING_CODES:
            tool_ref = st.text_input("Tool used (LOG/CAL only)", value="")

        ok = st.form_submit_button("Save activity", type="primary", use_container_width=True)
        if ok:
            storage.add_activity(
                shift_id=shift_id,
                start_ts=iso(start_dt),
                end_ts=iso(end_dt) if end_dt else None,
                code=code,
                title=title.strip() or CODES[code][0],
                notes=notes.strip(),
                tool_ref=tool_ref.strip(),
            )
            st.success("Saved.")
            st.session_state["dd_view"] = "list"
            st.rerun()

def dd_page(users, vehicles):
    dd_header()
    codes_help()

    # If no shift exists, force shift form first
    sh = storage.get_shift(st.session_state["shift_date"], st.session_state["shift_type"], st.session_state["username"])

    if sh is None:
        st.warning("No Daily Diary found for this day/shift. Fill in shift details to create it.")
        shift_form(vehicles, existing=None)
        return

    # Shift summary + edit gate
    st.markdown(
        f"<div class='card'>"
        f"<div class='title'>Shift summary</div>"
        f"<div class='muted'>Vehicle: <b>{sh.get('vehicle','')}</b> | Job: <b>{sh.get('job_number') or '-'}</b> | Site: <b>{sh.get('site_name') or '-'}</b> | Start: <b>{sh.get('shift_start') or '-'}</b> | Hours: <b>{sh.get('shift_hours') or 12}</b></div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    if st.session_state["dd_view"] == "edit_shift":
        shift_form(vehicles, existing=sh)
        return

    shift_id = int(sh["id"])
    if st.session_state["dd_view"] == "add":
        add_activity_view(shift_id)
    else:
        activities_list(shift_id)

def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    css()
    init_state()

    users = load_json_list("config/users.json", ["panza"])
    vehicles = load_json_list("config/vehicles.json", ["UNSET", "Hilux-01"])

    @st.cache_resource
    def _ensure_storage():
        storage.init_storage()
        return True

    _ensure_storage()


    if st.session_state["page"] == "login" or not st.session_state["username"]:
        login_page(users)
        return

    dd_page(users, vehicles)

if __name__ == "__main__":
    main()
