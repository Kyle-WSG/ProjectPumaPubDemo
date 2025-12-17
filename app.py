import json
import random
from io import BytesIO

import pandas as pd
from datetime import date as date_cls, datetime, timedelta, time as time_cls
from pathlib import Path
from typing import Any, Dict, List

import streamlit as st
import plotly.express as px

import storage

CONFIG = Path("config")
CLIENTS = ["RTIO", "RTC", "FMG", "FMGX", "Roy Hill", "Other"]
CODE_COLORS = {
    "LOG": "#C8102E",
    "CAL": "#F7931E",
    "SAF": "#2ECC71",
    "ADM": "#2E8AE6",
    "MTG": "#9B59B6",
    "DWN": "#7F8C8D",
    "OTH": "#95A5A6",
    "__editing__": "#ffd24d",
}
THEMES = {
    "dark": {
        "bg": "#0B111B",
        "card": "#111927",
        "panel": "#111927",
        "muted": "#A9B6C7",
        "text": "#E8EDF3",
        "accent": "#C8102E",
        "accent_alt": "#F7931E",
        "border": "rgba(255,255,255,0.12)",
        "shadow": "0 18px 38px rgba(0,0,0,0.45)",
    },
    "light": {
        "bg": "#F4F6FA",
        "card": "#FFFFFF",
        "panel": "#FFFFFF",
        "muted": "#5B6572",
        "text": "#0A1220",
        "accent": "#C8102E",
        "accent_alt": "#F7931E",
        "border": "rgba(0,0,0,0.08)",
        "shadow": "0 10px 24px rgba(0,0,0,0.08)",
    },
}
VERSION = "Alpha_2025_5"


def jload(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def load_users() -> List[str]:
    data = jload(CONFIG / "users.json", {"users": ["Operator"]})
    users = data.get("users", data) if isinstance(data, dict) else data
    users = [u.strip() for u in users if isinstance(u, str) and u.strip()]
    return users or ["Operator"]


def load_catalog() -> Dict[str, Any]:
    return jload(CONFIG / "catalog.json", {"activity_codes": [], "tools": []})


def load_vehicles() -> Dict[str, Dict[str, str]]:
    data = jload(CONFIG / "vehicles_catalog.json", {})
    out: Dict[str, Dict[str, str]] = {}
    for v in (data.get("vehicles", []) if isinstance(data, dict) else []):
        if not isinstance(v, dict):
            continue
        bc = str(v.get("barcode", "")).strip()
        name = str(v.get("name", "")).strip()
        if not bc or not name:
            continue
        out[bc] = {
            "barcode": bc,
            "name": name,
            "description": str(v.get("description", "")).strip(),
            "model": str(v.get("model", "")).strip(),
            "category": str(v.get("category", "")).strip() or "Vehicle",
            "location": str(v.get("location", "")).strip(),
        }
    return out


def site_options_from_vehicles(vehicles: Dict[str, Dict[str, str]]) -> List[str]:
    sites = {v.get("location", "").strip() for v in vehicles.values() if v.get("location")}
    sites.discard("")
    return sorted(sites)


def ensure_theme():
    if "theme" not in st.session_state:
        st.session_state.theme = "dark"


def toggle_theme():
    st.session_state.theme = "light" if st.session_state.get("theme") == "dark" else "dark"


def missing_shift_fields(sh: Dict[str, Any] | None) -> List[str]:
    required = ["client", "site", "job_number", "vehicle_barcode", "vehicle_name", "shift_start"]
    if not sh:
        return required + ["shift_end"]
    missing: List[str] = []
    for k in required:
        v = sh.get(k)
        if v is None or (isinstance(v, str) and not str(v).strip()):
            missing.append(k)
    # Only require manual site text if manual chosen
    if sh.get("site") in {"Other", "Other (manual)"} and not (sh.get("site_other") or "").strip():
        missing.append("site_other")
    if float(sh.get("shift_hours", 0) or 0) <= 0:
        missing.append("shift_end")
    return missing


def is_shift_complete(sh: Dict[str, Any] | None) -> bool:
    return len(missing_shift_fields(sh)) == 0


def fill_shift_defaults(sh: Dict[str, Any] | None, vehicles: Dict[str, Dict[str, str]], site_options: List[str]) -> tuple[Dict[str, Any] | None, bool]:
    """Auto-fill missing required fields with sane defaults (used for legacy rows)."""
    if not sh:
        return None, False
    updated = dict(sh)
    changed = False

    def ensure(key: str, val: Any):
        nonlocal changed
        if updated.get(key) is None or (isinstance(updated.get(key), str) and not str(updated.get(key)).strip()):
            updated[key] = val
            changed = True

    ensure("client", "Other")
    ensure("site", site_options[0] if site_options else "Other")
    if updated.get("site") in {"Other", "Other (manual)"} and not (updated.get("site_other") or "").strip():
        ensure("site_other", "Other")

    if not updated.get("vehicle_barcode") or not updated.get("vehicle_name"):
        first_vehicle = next(iter(sorted(vehicles.keys(), key=lambda x: (int(x) if str(x).isdigit() else 999999, x))), None)
        if first_vehicle and first_vehicle in vehicles:
            updated["vehicle_barcode"] = vehicles[first_vehicle].get("barcode") or first_vehicle
            updated["vehicle_name"] = vehicles[first_vehicle].get("name") or "Vehicle"
            updated["vehicle_category"] = vehicles[first_vehicle].get("category") or updated.get("vehicle_category")
        else:
            updated["vehicle_barcode"] = "UNKNOWN"
            updated["vehicle_name"] = "Vehicle"
    ensure("job_number", "UNKNOWN")
    ensure("shift_start", "06:00")
    if float(updated.get("shift_hours", 0) or 0) <= 0:
        updated["shift_hours"] = 12
        changed = True

    return updated, changed


def time_options(shift_date: date_cls, shift_start: str, shift_hours: float, step_minutes: int = 15) -> List[datetime]:
    try:
        base = dt_on(shift_date, time_cls.fromisoformat(shift_start))
    except Exception:
        base = dt_on(shift_date, time_cls.fromisoformat("06:00"))
    end = base + timedelta(hours=float(shift_hours or 12))
    opts = []
    cur = base
    while cur <= end:
        opts.append(cur)
        cur += timedelta(minutes=step_minutes)
    return opts


def format_time(dt: datetime) -> str:
    return dt.strftime("%H:%M")


def iso(d: date_cls) -> str:
    return d.isoformat()


def dt_on(d: date_cls, t: time_cls) -> datetime:
    return datetime(d.year, d.month, d.day, t.hour, t.minute)


def hangup_table_defaults(tools: List[str]) -> List[Dict[str, Any]]:
    if not tools:
        return [{"Tool": "Tool 1", "Hangup depth (m)": ""}]
    out = []
    for t in tools:
        out.append({"Tool": t, "Hangup depth (m)": ""})
    return out


def calibration_table_defaults() -> List[Dict[str, Any]]:
    return [{"Item": "", "Details": ""}]


def well_report_excel_bytes(report: Dict[str, Any], hangups: List[Dict[str, Any]], calibrations: List[Dict[str, Any]], dgps: Dict[str, Any], comments: str) -> bytes:
    buf = BytesIO()
    summary_rows = [{
        "Planned Northing/Easting": report.get("planned_northing_easting"),
        "Field": report.get("field"),
        "Site": report.get("site"),
        "Country": report.get("country"),
        "Mag Dec": report.get("mag_dec"),
        "Planned Dip": report.get("planned_dip"),
        "Planned Azimuth": report.get("planned_azimuth"),
        "Datum": report.get("datum"),
        "Grid Zone": report.get("grid_zone"),
        "Elevation": report.get("elevation"),
        "Drill Depth": report.get("drill_depth"),
        "Log Depth": report.get("log_depth"),
        "Hole ID": report.get("hole_id"),
        "IN DEVELOPMENT": "IN DEVELOPMENT",
    }]
    dgps_rows = [{
        "Easting": dgps.get("easting"),
        "Northing": dgps.get("northing"),
        "Zone": dgps.get("zone"),
        "Datum": dgps.get("datum"),
        "Elevation": dgps.get("elevation"),
        "Accuracy": dgps.get("accuracy"),
    }]
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Well Report", index=False)
        pd.DataFrame(hangups).to_excel(writer, sheet_name="Hangup Depths", index=False)
        pd.DataFrame(calibrations).to_excel(writer, sheet_name="Calibration", index=False)
        pd.DataFrame(dgps_rows).to_excel(writer, sheet_name="DGPS", index=False)
        pd.DataFrame([{"Comments": comments or ""}]).to_excel(writer, sheet_name="Comments", index=False)
    return buf.getvalue()


def demo_well_report_defaults(act: Dict[str, Any]) -> Dict[str, Any]:
    """Generate placeholder well report values for demo purposes."""
    depth_from = round(random.uniform(50, 250), 1)
    depth_to = round(depth_from + random.uniform(10, 40), 1)
    return {
        "planned_northing_easting": f"{random.randint(700000, 799999)}E {random.randint(7400000, 7499999)}N",
        "field": "Demo Field",
        "site": "STE-01",
        "country": "AU",
        "mag_dec": random.choice([0.0, 1.2, 1.5]),
        "planned_dip": -60,
        "planned_azimuth": 45,
        "datum": "MGA94",
        "grid_zone": "50K",
        "elevation": round(random.uniform(300, 550), 1),
        "drill_depth": depth_to + 20,
        "log_depth": depth_to,
        "hole_id": act.get("hole_id") or f"H-{random.randint(100, 999)}",
        "comments": "Auto-generated for demo purposes. Adjust before finalising.",
        "inj_development": "IN DEVELOPMENT",
        "dgps_easting": random.randint(700000, 799999),
        "dgps_northing": random.randint(7400000, 7499999),
        "dgps_zone": "50K",
        "dgps_datum": "MGA94",
        "dgps_elevation": round(random.uniform(300, 550), 1),
        "dgps_accuracy": "±0.05m",
    }


def shift_progress(shift: Dict[str, Any], acts: List[Dict[str, Any]]) -> float:
    try:
        d = datetime.fromisoformat(shift["shift_date"]).date()
        h, m = [int(x) for x in str(shift.get("shift_start", "06:00")).split(":")]
    except Exception:
        return 0.0
    start = datetime(d.year, d.month, d.day, h, m)
    end = start + timedelta(hours=float(shift.get("shift_hours", 12)))
    total = max(1, int((end - start).total_seconds() // 60))
    logged = 0
    for a in acts:
        try:
            a0 = datetime.fromisoformat(a.get("start_ts"))
            a1 = datetime.fromisoformat(a.get("end_ts"))
        except Exception:
            continue
        lo = max(a0, start)
        hi = min(a1, end)
        if hi > lo:
            logged += int((hi - lo).total_seconds() // 60)
    return max(0.0, min(1.0, logged / total))


def activity_timeline(shift: Dict[str, Any], acts: List[Dict[str, Any]], highlight_id: int | None = None):
    """Render a single-row timeline with segments for each activity, showing full shift window."""
    try:
        d = datetime.fromisoformat(shift["shift_date"]).date()
        h, m = [int(x) for x in str(shift.get("shift_start", "06:00")).split(":")]
    except Exception:
        st.warning("Cannot render timeline – invalid shift start.")
        return

    start = datetime(d.year, d.month, d.day, h, m)
    end = start + timedelta(hours=float(shift.get("shift_hours", 12)))

    rows = [{
        "Lane": "Shift",
        "Start": start,
        "End": end,
        "Code": "__shift__",
        "Label": "",
        "Notes": "",
    }]
    for a in acts:
        try:
            a0 = datetime.fromisoformat(a.get("start_ts"))
            a1 = datetime.fromisoformat(a.get("end_ts"))
        except Exception:
            continue
        lo = max(a0, start)
        hi = min(a1, end)
        if hi <= lo:
            continue
        code_val = a.get("code")
        label_val = a.get("label") or a.get("title") or ""
        if highlight_id is not None and int(a.get("id")) == int(highlight_id):
            code_val = "__editing__"
            label_val = label_val or "Editing"
        rows.append({
            "Lane": "Shift",
            "Start": lo,
            "End": hi,
            "Code": code_val,
            "Label": label_val,
            "Notes": a.get("notes") or "",
        })

    if not rows:
        st.info("No in-window activities to display.")
        return

    color_map = {"__shift__": "rgba(255,255,255,0.08)", "__editing__": CODE_COLORS.get("__editing__", "#ffd24d")}
    for k, v in CODE_COLORS.items():
        if k.startswith("__"):
            continue
        color_map.setdefault(k, v)
    fig = px.timeline(
        rows,
        x_start="Start",
        x_end="End",
        y="Lane",
        color="Code",
        hover_data={"Label": True, "Start": True, "End": True},
        text="Label",
        color_discrete_map=color_map,
    )
    fig.update_yaxes(visible=False, showticklabels=False)
    fig.update_layout(
        height=180,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis_title=None,
        showlegend=True,
        hovermode="x",
    )
    fig.update_xaxes(range=[start, end], dtick=60 * 60 * 1000, tickformat="%H:%M", showgrid=True, gridcolor="rgba(255,255,255,0.10)", griddash="dot")
    fig.update_traces(textposition="inside", insidetextanchor="middle", textfont_size=11, marker_line_width=0)
    for tr in fig.data:
        if tr.name == "__shift__":
            tr.showlegend = False
            tr.marker.color = "rgba(200,16,46,0.20)"
            tr.marker.line.width = 0
        if tr.name == "__editing__":
            tr.name = "Editing"
    st.plotly_chart(fig, use_container_width=True, theme="streamlit")


def style(theme: str):
    palette = THEMES.get(theme, THEMES["dark"])
    # Align Plotly template with theme
    try:
        import plotly.io as pio  # type: ignore
        pio.templates.default = "plotly_dark" if theme == "dark" else "plotly_white"
    except Exception:
        pass
    st.markdown(
        f"""
        <style>
          :root {{
            --wsg-red: {palette["accent"]};
            --wsg-red-alt: #d84f4f;
            --wsg-bg: {palette["bg"]};
            --wsg-card: {palette["card"]};
            --wsg-panel: {palette["panel"]};
            --wsg-muted: {palette["muted"]};
            --wsg-text: {palette["text"]};
            --wsg-border: {palette["border"]};
            --wsg-shadow: {palette["shadow"]};
          }}
          body {{
            background: radial-gradient(circle at 18% 22%, rgba(200,16,46,0.08), transparent 36%),
                        radial-gradient(circle at 82% 4%, rgba(241,147,30,0.10), transparent 38%),
                        var(--wsg-bg);
            color: var(--wsg-text);
          }}
          /* Extra top padding so content clears the Streamlit deploy bar */
          .block-container {{padding-top: 3.8rem; padding-bottom: 3rem; max-width: 1200px;}}
          .card {{background: var(--wsg-card); border:1px solid var(--wsg-border); border-radius: 18px; padding: 16px; box-shadow: var(--wsg-shadow); backdrop-filter: blur(6px);}}
          .pill {{border-radius: 999px; padding: 6px 12px; background: rgba(0,0,0,0.04); font-size: 0.85rem; display: inline-block; margin-right: 6px; margin-bottom: 6px; color: var(--wsg-muted); border: 1px solid var(--wsg-border);}}
          .muted {{opacity:0.82; color: var(--wsg-muted);}}
          .title-lg {{font-size: 1.45rem; font-weight: 700;}}
          .title-md {{font-size: 1.1rem; font-weight: 700; line-height: 1.1; margin:0;}}
          /* Scoped button styles inside the main app container to avoid touching Streamlit chrome */
          .block-container .stButton>button,
          .block-container button[kind] {{
            background: linear-gradient(135deg, var(--wsg-red), var(--wsg-red-alt));
            color: #fff;
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 12px;
            padding: 10px 16px;
            box-shadow: 0 10px 24px rgba(200,16,46,0.22);
            transition: all 0.18s ease;
            font-weight: 700;
          }}
          .block-container .stButton>button:hover,
          .block-container button[kind]:hover {{
            transform: translateY(-1px);
            box-shadow: 0 14px 30px rgba(200,16,46,0.3);
            border-color: rgba(255,255,255,0.18);
          }}
          .block-container .stButton>button:active,
          .block-container button[kind]:active {{
            transform: translateY(0);
            box-shadow: 0 8px 18px rgba(200,16,46,0.2);
          }}
          .block-container button[kind="secondary"] {{
            background: linear-gradient(135deg, #1f2735, #2a3244);
            color: #e8ecf2;
            border: 1px solid rgba(255,255,255,0.10);
            box-shadow: none;
          }}
          .block-container button[kind="secondary"]:hover {{
            background: linear-gradient(135deg, #242d3d, #313a4f);
          }}
          .block-container .stDownloadButton>button {{
            background: linear-gradient(135deg, #1f8c6c, #26a985);
            box-shadow: 0 10px 24px rgba(38,169,133,0.25);
            border: none;
          }}
          .block-container .stDownloadButton>button:hover {{
            box-shadow: 0 14px 30px rgba(38,169,133,0.32);
          }}
          .tight-row {{display:flex; gap:10px; flex-wrap:wrap;}}
          .chip {{padding: 6px 10px; border-radius: 10px; border: 1px solid var(--wsg-border); font-size: 0.9rem; color: var(--wsg-muted);}}
          .icon-btn button {{background: transparent !important; border: 1px solid var(--wsg-border); color: var(--wsg-muted);}}
          .stDataFrame, .stDataEditor {{border-radius: 14px; border:1px solid var(--wsg-border); box-shadow: var(--wsg-shadow);}}
          .stSelectbox>div>div {{border-radius: 12px !important; border:1px solid var(--wsg-border) !important;}}
          input, textarea {{border-radius: 10px !important; border:1px solid var(--wsg-border) !important; box-shadow:none !important;}}
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_resource
def boot(vehicles: Dict[str, Dict[str, str]]):
    storage.init_storage()
    storage.upsert_reference_data(list(vehicles.values()))
    return True


def login(users: List[str]):
    st.markdown("<div class='title-lg'>Project Puma</div>", unsafe_allow_html=True)
    st.caption("Wireline daily diary — one shift per user per day.")
    user = st.selectbox("User", users)
    if st.button("Enter", type="primary", use_container_width=True):
        st.session_state.username = user
        st.session_state.shift_date = iso(date_cls.today())
        st.session_state.view = "dd"
        st.rerun()


def topbar():
    d = datetime.fromisoformat(st.session_state.shift_date).date()
    with st.container(border=True):
        c_user, c_prev, c_date, c_next, c_today, c_logout = st.columns([1.6, 0.7, 3.0, 0.7, 1.0, 1.1], vertical_alignment="center")
        with c_user:
            st.markdown(f"<div class='title-md' style='text-align:center; padding:8px 0; display:flex; align-items:center; justify-content:center;'>{st.session_state.username}</div>", unsafe_allow_html=True)
        with c_prev:
            if st.button("◀", use_container_width=True):
                st.session_state.shift_date = iso(d - timedelta(days=1)); st.session_state.view = "dd"; st.rerun()
        with c_date:
            st.markdown(f"<div class='title-md' style='text-align:center; padding:8px 0; display:flex; align-items:center; justify-content:center;'>{d.strftime('%a %d %b %Y')}</div>", unsafe_allow_html=True)
        with c_next:
            if st.button("▶", use_container_width=True):
                st.session_state.shift_date = iso(d + timedelta(days=1)); st.session_state.view = "dd"; st.rerun()
        with c_today:
            if st.button("Today", use_container_width=True):
                st.session_state.shift_date = iso(date_cls.today()); st.session_state.view = "dd"; st.rerun()
        with c_logout:
            if st.button("Log out", use_container_width=True):
                for k in ["username", "shift_date", "view", "edit_activity_id", "activity_code_select", "act_start_iso", "act_end_iso"]:
                    st.session_state.pop(k, None)
                st.rerun()


def shift_form(vehicles: Dict[str, Dict[str, str]], site_options: List[str], existing: Dict[str, Any] | None = None, missing: List[str] | None = None, form_key: str = "shift_form"):
    existing = existing or {}
    d = st.session_state.shift_date
    username = st.session_state.username
    missing = missing or []

    with st.form(form_key):
        st.markdown("### Shift details")
        c1, c2 = st.columns([1.1, 1.1])
        with c1:
            client = st.selectbox("Client *", CLIENTS, index=(CLIENTS.index(existing.get("client", "Other")) if existing.get("client") in CLIENTS else CLIENTS.index("Other")))
            if "client" in missing:
                st.caption(":red[Required]")
        with c2:
            job = st.text_input("Job number *", value=str(existing.get("job_number", "")), placeholder="Required")
            if "job_number" in missing:
                st.caption(":red[Required]")

        site_list = site_options + (["Other (manual)"] if "Other (manual)" not in site_options else [])
        site_val = existing.get("site") if existing.get("site") else (site_options[0] if site_options else "Other (manual)")
        site = st.selectbox("Site *", site_list, index=(site_list.index(site_val) if site_val in site_list else site_list.index("Other (manual)")))
        site_other = ""
        if site == "Other (manual)":
            site_other = st.text_input("Site (manual) *", value=str(existing.get("site_other", "")), placeholder="Required")
            if "site_other" in missing:
                st.caption(":red[Required]")

        st.markdown("#### Vehicle")
        options = ["__OTHER__"] + sorted(vehicles.keys(), key=lambda x: (int(x) if str(x).isdigit() else 999999, x)) if vehicles else ["__OTHER__"]
        fmt = lambda bc: "Other / not listed" if bc == "__OTHER__" else f"{bc} — {vehicles[bc].get('name','')} ({vehicles[bc].get('category','')})"
        current_bc = existing.get("vehicle_barcode") or (options[1] if len(options) > 1 else "__OTHER__")
        vbc = st.selectbox("Vehicle *", options, format_func=fmt, index=(options.index(current_bc) if current_bc in options else 0))

        vehicle_data = vehicles.get(vbc, {}) if vbc != "__OTHER__" else {}
        expected_loc = vehicle_data.get("location", "")
        if vbc == "__OTHER__":
            st.info("Enter the new vehicle details; this will be stored with the shift.")
            vbc = st.text_input("Vehicle barcode *", value=str(existing.get("vehicle_barcode", "")), placeholder="Required")
            if "vehicle_barcode" in missing:
                st.caption(":red[Required]")
            vname = st.text_input("Vehicle name *", value=str(existing.get("vehicle_name", "")), placeholder="Required")
            if "vehicle_name" in missing:
                st.caption(":red[Required]")
            vcat = st.text_input("Category", value=str(existing.get("vehicle_category", "")))
            vdesc = st.text_input("Description", value=str(existing.get("vehicle_description", "")))
            vmodel = st.text_input("Model", value=str(existing.get("vehicle_model", "")))
            expected_loc = ""
        else:
            vname = vehicle_data.get("name", "")
            vcat = vehicle_data.get("category", "")
            vdesc = vehicle_data.get("description", "")
            vmodel = vehicle_data.get("model", "")

        t_start_default = datetime.now().time().replace(second=0, microsecond=0) if not existing else time_cls.fromisoformat(existing.get("shift_start", "06:00"))
        shift_hours_existing = float(existing.get("shift_hours", 12))
        if shift_hours_existing <= 0:
            shift_hours_existing = 12
        try:
            base_dt = dt_on(datetime.fromisoformat(d).date(), time_cls.fromisoformat(existing.get("shift_start", "06:00")))
            end_dt_default = (base_dt + timedelta(hours=shift_hours_existing)).time()
        except Exception:
            end_dt_default = (datetime.now() + timedelta(hours=12)).time().replace(second=0, microsecond=0)
        t_start = st.time_input("Shift start *", value=t_start_default)
        if "shift_start" in missing:
            st.caption(":red[Required]")
        t_end = st.time_input("Shift end *", value=end_dt_default)
        if "shift_end" in missing:
            st.caption(":red[Required]")
        notes = st.text_area("Shift notes (optional)", value=str(existing.get("shift_notes", "")), height=90)

        ok = st.form_submit_button("Save shift", type="primary", use_container_width=True)
        if not ok:
            return

        errs = []
        if not job.strip():
            errs.append("Job number required.")
        if site == "Other (manual)" and not site_other.strip():
            errs.append("Site (manual) required.")
        if not vbc or not vname:
            errs.append("Vehicle barcode and name required.")
        shift_hours = (dt_on(datetime.fromisoformat(d).date(), t_end) - dt_on(datetime.fromisoformat(d).date(), t_start)).total_seconds() / 3600
        if shift_hours <= 0:
            shift_hours += 24  # handle wrap past midnight
        if shift_hours <= 0:
            errs.append("Shift end must be after start.")
        if errs:
            st.error(" ".join(errs))
            return

        saved = storage.upsert_shift({
            "shift_date": d,
            "username": username,
            "client": client,
            "site": site if site != "Other (manual)" else "Other",
            "site_other": site_other.strip() if site == "Other (manual)" else None,
            "job_number": job.strip(),
            "vehicle_barcode": vbc.strip(),
            "vehicle_name": vname.strip(),
            "vehicle_description": vdesc.strip() if vdesc else None,
            "vehicle_model": vmodel.strip() if vmodel else None,
            "vehicle_category": vcat.strip() if vcat else None,
            "vehicle_location_expected": expected_loc.strip() if expected_loc else None,
            "vehicle_location_actual": expected_loc.strip() if expected_loc else None,
            "vehicle_location_mismatch": False,
            "shift_start": t_start.strftime("%H:%M"),
            "shift_hours": shift_hours,
            "shift_notes": notes.strip() if notes.strip() else None,
        })
        st.session_state.latest_shift = saved
        st.session_state.view = "dd"
        st.success("Shift saved.")
        st.rerun()


def add_activity_form(catalog: Dict[str, Any], sh: Dict[str, Any], acts: List[Dict[str, Any]]):
    codes = catalog.get("activity_codes", []) or []
    tools = catalog.get("tools", []) or []
    if not tools:
        tools = ["Natural Gamma", "Density", "Neutron", "Other / Notes"]
    code_list = [c["code"] for c in codes if isinstance(c, dict) and c.get("code")]
    label_by = {c["code"]: c.get("label", c.get("code")) for c in codes if isinstance(c, dict) and c.get("code")}
    if not code_list:
        code_list = ["LOG", "CAL", "SAF", "ADM", "MTG", "DWN", "OTH"]
        label_by = {c: c for c in code_list}

    # Choose code outside the form so the UI re-renders immediately
    if "activity_code_select" not in st.session_state:
        st.session_state.activity_code_select = code_list[0]
    code_choice = st.selectbox("Code", code_list, index=code_list.index(st.session_state.activity_code_select) if st.session_state.activity_code_select in code_list else 0, key="activity_code_select")
    st.caption(f"**{code_choice}** — {label_by.get(code_choice, code_choice)}")

    d = datetime.fromisoformat(st.session_state.shift_date).date()
    shift_start = sh.get("shift_start", "06:00")
    shift_hours = float(sh.get("shift_hours", 12))
    options = time_options(d, shift_start, shift_hours)
    option_ids = [o.isoformat() for o in options]

    # Find first available 30-min slot that doesn't overlap existing acts
    def first_available_slot() -> tuple[datetime, datetime]:
        intervals = []
        for a in acts:
            try:
                s0 = datetime.fromisoformat(a.get("start_ts"))
                s1 = datetime.fromisoformat(a.get("end_ts"))
                if s1 > s0:
                    intervals.append((s0, s1))
            except Exception:
                continue
        intervals.sort()
        for i, start in enumerate(options):
            if i == len(options) - 1:
                break
            candidate_start = start
            candidate_end = min(options[-1], candidate_start + timedelta(minutes=30))
            if candidate_end <= candidate_start:
                continue
            overlap = False
            for lo, hi in intervals:
                if max(candidate_start, lo) < min(candidate_end, hi):
                    overlap = True
                    break
            if not overlap:
                return candidate_start, candidate_end
        # fallback to shift start + 30
        fallback_start = options[0]
        fallback_end = min(options[-1], fallback_start + timedelta(minutes=30))
        if fallback_end <= fallback_start and len(options) > 1:
            fallback_end = options[1]
        return fallback_start, fallback_end

    slot_start, slot_end = first_available_slot()

    now_dt = datetime.now()
    default_start = max(options[0], min(now_dt.replace(second=0, microsecond=0), options[-1]))
    # Snap default start to nearest option
    default_start = min(options, key=lambda x: abs((x - default_start).total_seconds()))
    default_end_target = min(options[-1], default_start + timedelta(minutes=30))
    default_end = min(options, key=lambda x: abs((x - default_end_target).total_seconds()))
    if default_end <= default_start and len(options) > 1:
        default_end = options[min(options.index(default_start) + 1, len(options) - 1)]

    with st.form("act_form", clear_on_submit=False):
        c1, c2 = st.columns([1.0, 1.0])
        start_default_id = st.session_state.get("act_start_iso")
        end_default_id = st.session_state.get("act_end_iso")
        # Seed defaults to first available slot if nothing stored
        if not start_default_id or start_default_id not in option_ids:
            start_default_id = slot_start.isoformat()
        if not end_default_id or end_default_id not in option_ids:
            end_default_id = slot_end.isoformat()
        if start_default_id not in option_ids:
            start_default_id = default_start.isoformat()
        if end_default_id not in option_ids:
            end_default_id = default_end.isoformat()
        with c1:
            start_id = st.selectbox("Start", option_ids, format_func=lambda s: format_time(datetime.fromisoformat(s)), index=option_ids.index(start_default_id) if start_default_id in option_ids else 0, key="act_start_iso")
        with c2:
            end_id = st.selectbox("End", option_ids, format_func=lambda s: format_time(datetime.fromisoformat(s)), index=option_ids.index(end_default_id) if end_default_id in option_ids else min(len(option_ids)-1, option_ids.index(start_default_id)+1 if start_default_id in option_ids else 1), key="act_end_iso")

        selected_tools: List[str] = []
        if code_choice in {"LOG", "CAL"}:
            default_tools = st.session_state.get("tool_select_multi", [])
            if not isinstance(default_tools, list):
                default_tools = []
            if not default_tools and tools:
                default_tools = [tools[0]]
            selected_tools = st.multiselect("Tools (LOG/CAL only)", tools, default=default_tools, key="tool_select_multi")
        else:
            st.session_state.pop("tool_select_multi", None)
        hole_id = st.text_input("Hole ID (optional)", placeholder="Enter hole ID or leave blank")
        notes = st.text_area("Notes (optional)", height=80)

        ok = st.form_submit_button("Add activity", type="primary", use_container_width=True)
        if not ok:
            return

        a0 = datetime.fromisoformat(start_id)
        a1 = datetime.fromisoformat(end_id)
        if a1 <= a0:
            st.error("End must be after start.")
            return

        for existing in acts:
            try:
                e0 = datetime.fromisoformat(existing.get("start_ts"))
                e1 = datetime.fromisoformat(existing.get("end_ts"))
            except Exception:
                continue
            if max(a0, e0) < min(a1, e1):
                st.error(f"Time conflict with {existing.get('code')} — {existing.get('label')} ({existing.get('start_ts')} → {existing.get('end_ts')}).")
                return

        storage.add_activity(st.session_state.shift_date, st.session_state.username, {
            "start_ts": a0.isoformat(timespec="seconds"),
            "end_ts": a1.isoformat(timespec="seconds"),
            "code": code_choice,
            "label": label_by.get(code_choice, code_choice),
            "tool": (", ".join([t for t in selected_tools if str(t).strip()]) if selected_tools else None),
            "hole_id": hole_id.strip() if hole_id.strip() else None,
            "notes": notes.strip() if notes.strip() else None,
        })
        st.session_state.view = "dd"
        st.success("Activity added.")
        st.rerun()


def edit_activity_form(catalog: Dict[str, Any], sh: Dict[str, Any], acts: List[Dict[str, Any]], act: Dict[str, Any]):
    codes = catalog.get("activity_codes", []) or []
    tools = catalog.get("tools", []) or []
    if not tools:
        tools = ["Natural Gamma", "Density", "Neutron", "Other / Notes"]
    code_list = [c["code"] for c in codes if isinstance(c, dict) and c.get("code")]
    label_by = {c["code"]: c.get("label", c.get("code")) for c in codes if isinstance(c, dict) and c.get("code")}
    if not code_list:
        code_list = ["LOG", "CAL", "SAF", "ADM", "MTG", "DWN", "OTH"]
        label_by = {c: c for c in code_list}

    d = datetime.fromisoformat(st.session_state.shift_date).date()
    shift_start = sh.get("shift_start", "06:00")
    shift_hours = float(sh.get("shift_hours", 12))
    options = time_options(d, shift_start, shift_hours)
    option_ids = [o.isoformat() for o in options]

    code_default = act.get("code") or code_list[0]
    code_choice = st.selectbox("Code", code_list, index=(code_list.index(code_default) if code_default in code_list else 0), key="edit_code_select")
    st.caption(f"**{code_choice}** — {label_by.get(code_choice, code_choice)}")

    if st.button("Cancel editing", type="secondary"):
        st.session_state.view = "dd"
        st.session_state.edit_activity_id = None
        st.rerun()

    if code_choice == "LOG":
        if st.button("Well report", key=f"wr_open_{act.get('id')}", use_container_width=True):
            st.session_state.well_report_activity_id = int(act.get("id"))
            st.session_state.view = "well_report"
            st.rerun()

    with st.form("edit_act_form", clear_on_submit=False):
        c1, c2 = st.columns([1.0, 1.0])
        start_iso = act.get("start_ts") or (act.get("start_time") if act else None)
        end_iso = act.get("end_ts") or (act.get("end_time") if act else None)
        if start_iso not in option_ids:
            start_iso = option_ids[0]
        if end_iso not in option_ids:
            end_iso = option_ids[min(len(option_ids) - 1, option_ids.index(start_iso) + 1)]
        with c1:
            start_id = st.selectbox("Start", option_ids, format_func=lambda s: format_time(datetime.fromisoformat(s)), index=option_ids.index(start_iso), key="edit_act_start_iso")
        with c2:
            end_id = st.selectbox("End", option_ids, format_func=lambda s: format_time(datetime.fromisoformat(s)), index=option_ids.index(end_iso), key="edit_act_end_iso")

        selected_tools: List[str] = []
        if code_choice in {"LOG", "CAL"}:
            existing_tools = []
            if act.get("tool"):
                existing_tools = [t.strip() for t in str(act.get("tool")).split(",") if t.strip()]
            default_tools = st.session_state.get("tool_select_edit_multi", existing_tools)
            if not isinstance(default_tools, list):
                default_tools = existing_tools
            selected_tools = st.multiselect("Tools (LOG/CAL only)", tools, default=default_tools, key="tool_select_edit_multi")
        else:
            st.session_state.pop("tool_select_edit_multi", None)

        hole_id_val = st.text_input("Hole ID (optional)", value=str(act.get("hole_id") or ""))
        notes = st.text_area("Notes (optional)", height=80, value=str(act.get("notes") or ""))

        ok = st.form_submit_button("Save changes", type="primary", use_container_width=True)
        if not ok:
            return

        a0 = datetime.fromisoformat(start_id)
        a1 = datetime.fromisoformat(end_id)
        if a1 <= a0:
            st.error("End must be after start.")
            return

        for existing in acts:
            if existing.get("id") == act.get("id"):
                continue
            try:
                e0 = datetime.fromisoformat(existing.get("start_ts"))
                e1 = datetime.fromisoformat(existing.get("end_ts"))
            except Exception:
                continue
            if max(a0, e0) < min(a1, e1):
                st.error(f"Time conflict with {existing.get('code')} — {existing.get('label')} ({existing.get('start_ts')} → {existing.get('end_ts')}).")
                return

        storage.update_activity(st.session_state.shift_date, st.session_state.username, int(act.get("id")), {
            "start_ts": a0.isoformat(timespec="seconds"),
            "end_ts": a1.isoformat(timespec="seconds"),
            "code": code_choice,
            "label": label_by.get(code_choice, code_choice),
            "tool": (", ".join([t for t in selected_tools if str(t).strip()]) if selected_tools else (act.get("tool") if code_choice not in {"LOG", "CAL"} else None)),
            "hole_id": hole_id_val.strip() if hole_id_val.strip() else None,
            "notes": notes.strip() if notes.strip() else None,
        })
        st.session_state.view = "dd"
        st.session_state.edit_activity_id = None
        st.success("Activity updated.")
        st.rerun()


def well_report_view(act: Dict[str, Any]):
    """Render the well report editor for a LOG activity."""
    if not act or act.get("code") != "LOG":
        st.warning("Well report is only available for LOG activities.")
        st.session_state.view = "dd"
        st.session_state.well_report_activity_id = None
        st.rerun()

    tools = [t.strip() for t in str(act.get("tool") or "").split(",") if t.strip()]
    aid = int(act.get("id"))
    data_key = f"wr_data_{aid}"
    hang_key = f"wr_hang_{aid}"
    calib_key = f"wr_calib_{aid}"

    if data_key not in st.session_state:
        st.session_state[data_key] = demo_well_report_defaults(act)
    if hang_key not in st.session_state:
        st.session_state[hang_key] = hangup_table_defaults(tools)
    if calib_key not in st.session_state:
        st.session_state[calib_key] = calibration_table_defaults()

    data = st.session_state.get(data_key, {})
    hangups = st.session_state.get(hang_key, [])
    calibrations = st.session_state.get(calib_key, [])

    st.markdown(f"## Well report — {act.get('label')} ({act.get('code')})")
    st.caption("IN DEVELOPMENT")

    c1, c2 = st.columns(2)
    with c1:
        pne = st.text_input("Planned Northing/Easting", value=str(data.get("planned_northing_easting", "")), key=f"wr_pne_{aid}")
        field = st.text_input("Field", value=str(data.get("field", "")), key=f"wr_field_{aid}")
        site = st.text_input("Site", value=str(data.get("site", "")), key=f"wr_site_{aid}")
        country = st.text_input("Country", value=str(data.get("country", "")), key=f"wr_country_{aid}")
        mag_dec = st.number_input("Mag Dec", value=float(data.get("mag_dec") or 0.0), step=0.1, format="%.2f", key=f"wr_magdec_{aid}")
        plan_dip = st.number_input("Planned Dip", value=float(data.get("planned_dip") or 0.0), step=1.0, format="%.2f", key=f"wr_pd_{aid}")
    with c2:
        plan_az = st.number_input("Planned Azimuth", value=float(data.get("planned_azimuth") or 0.0), step=1.0, format="%.2f", key=f"wr_paz_{aid}")
        datum = st.text_input("Datum", value=str(data.get("datum", "")), key=f"wr_datum_{aid}")
        grid = st.text_input("Grid Zone", value=str(data.get("grid_zone", "")), key=f"wr_grid_{aid}")
        elev = st.number_input("Elevation", value=float(data.get("elevation") or 0.0), step=0.1, format="%.2f", key=f"wr_elev_{aid}")
        drill = st.number_input("Drill Depth", value=float(data.get("drill_depth") or 0.0), step=1.0, format="%.2f", key=f"wr_drill_{aid}")
        log_depth = st.number_input("Log Depth", value=float(data.get("log_depth") or 0.0), step=1.0, format="%.2f", key=f"wr_log_{aid}")
    hole_id = st.text_input("Hole ID", value=str(data.get("hole_id", "")), key=f"wr_hole_{aid}")

    st.markdown("**Hangup depths (per tool)**")
    hangups = st.data_editor(hangups or hangup_table_defaults(tools), key=f"wr_hang_table_{aid}", num_rows="dynamic", use_container_width=True)

    st.markdown("**Calibration details** (leave blank if none)")
    calibrations = st.data_editor(calibrations or [], key=f"wr_calib_table_{aid}", num_rows="dynamic", use_container_width=True)

    st.markdown("**DGPS**")
    c3, c4, c5 = st.columns(3)
    with c3:
        dgps_e = st.number_input("Easting", value=float(data.get("dgps_easting") or 0.0), step=1.0, key=f"wr_dgps_e_{aid}")
        dgps_zone = st.text_input("Zone", value=str(data.get("dgps_zone", "")), key=f"wr_dgps_zone_{aid}")
    with c4:
        dgps_n = st.number_input("Northing", value=float(data.get("dgps_northing") or 0.0), step=1.0, key=f"wr_dgps_n_{aid}")
        dgps_datum = st.text_input("Datum (DGPS)", value=str(data.get("dgps_datum", "")), key=f"wr_dgps_datum_{aid}")
    with c5:
        dgps_el = st.number_input("Elevation (DGPS)", value=float(data.get("dgps_elevation") or 0.0), step=0.1, format="%.2f", key=f"wr_dgps_el_{aid}")
        dgps_acc = st.text_input("Accuracy", value=str(data.get("dgps_accuracy", "")), key=f"wr_dgps_acc_{aid}")

    comments = st.text_area("Comments", value=str(data.get("comments", "")), height=100, key=f"wr_comments_{aid}")

    updated_data = {
        "planned_northing_easting": pne,
        "field": field,
        "site": site,
        "country": country,
        "mag_dec": mag_dec,
        "planned_dip": plan_dip,
        "planned_azimuth": plan_az,
        "datum": datum,
        "grid_zone": grid,
        "elevation": elev,
        "drill_depth": drill,
        "log_depth": log_depth,
        "hole_id": hole_id,
        "comments": comments,
        "inj_development": "IN DEVELOPMENT",
        "dgps_easting": dgps_e,
        "dgps_northing": dgps_n,
        "dgps_zone": dgps_zone,
        "dgps_datum": dgps_datum,
        "dgps_elevation": dgps_el,
        "dgps_accuracy": dgps_acc,
    }
    dgps_data = {
        "easting": dgps_e,
        "northing": dgps_n,
        "zone": dgps_zone,
        "datum": dgps_datum,
        "elevation": dgps_el,
        "accuracy": dgps_acc,
    }

    st.session_state[data_key] = updated_data
    st.session_state[hang_key] = hangups
    st.session_state[calib_key] = calibrations

    btn1, btn2, btn3 = st.columns([1, 1, 1])
    save_clicked = btn1.button("Save", type="primary", use_container_width=True)
    dl_bytes = well_report_excel_bytes(updated_data, hangups, calibrations, dgps_data, comments)
    btn2.download_button("Download (Excel)", data=dl_bytes, file_name="well_report_demo.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
    exit_clicked = btn3.button("Exit", type="secondary", use_container_width=True)

    if save_clicked:
        st.success("Well report saved.")
    if exit_clicked:
        st.session_state.view = "edit_activity"
        st.session_state.well_report_activity_id = None
        st.rerun()


def main():
    st.set_page_config(page_title="Project Puma", layout="wide")
    ensure_theme()
    style(st.session_state.theme)

    users = load_users()
    catalog = load_catalog()
    vehicles = load_vehicles()
    site_options = site_options_from_vehicles(vehicles)

    boot(vehicles)

    if "username" not in st.session_state:
        login(users)
        return

    topbar()
    st.divider()

    sh = storage.get_shift(st.session_state.shift_date, st.session_state.username)
    latest = st.session_state.get("latest_shift")
    if latest and latest.get("shift_date") == st.session_state.shift_date and latest.get("username") == st.session_state.username:
        if not sh or latest.get("updated_at") >= sh.get("updated_at", ""):
            sh = latest
    if sh:
        patched, changed = fill_shift_defaults(sh, vehicles, site_options)
        if changed and patched:
            sh = storage.upsert_shift(patched)
    if sh is None:
        st.markdown("## Create shift")
        st.info("One shift per user per day. Client + site + job number + vehicle are required.")
        shift_form(vehicles, site_options, form_key="shift_form_create")
        return

    if not is_shift_complete(sh):
        missing = missing_shift_fields(sh)
        st.markdown("## Complete shift details")
        st.warning("Fill in all shift details before adding or viewing activities.")
        if missing:
            human = {
                "client": "Client",
                "site": "Site",
                "site_other": "Site (manual)",
                "job_number": "Job number",
                "vehicle_barcode": "Vehicle barcode",
                "vehicle_name": "Vehicle name",
                "shift_start": "Shift start",
                "shift_end": "Shift end",
            }
            st.error("Missing: " + ", ".join([human.get(m, m) for m in missing]))
        st.session_state.view = "edit_shift"
        shift_form(vehicles, site_options, existing=sh, missing=missing, form_key="shift_form_incomplete")
        return

    site_display = sh.get("site_other") if sh.get("site") == "Other" and sh.get("site_other") else sh.get("site")
    try:
        start_dt = dt_on(datetime.fromisoformat(sh.get("shift_date")).date(), time_cls.fromisoformat(sh.get("shift_start")))
        end_dt = start_dt + timedelta(hours=float(sh.get("shift_hours", 12)))
        end_str = end_dt.strftime("%H:%M")
    except Exception:
        end_str = "—"
    with st.container():
        col_info, col_edit = st.columns([4, 1], vertical_alignment="center")
        with col_info:
            st.markdown(
                f"""
                <div class="card">
                  <div class="title-md">{st.session_state.get('username', sh.get('username'))} — {datetime.fromisoformat(sh.get('shift_date')).strftime('%a %d %b %Y')}</div>
                  <div class="muted" style="margin-bottom:6px;">Location: {site_display}</div>
                  <div class="tight-row">
                    <span class="pill">Client: {sh.get('client')}</span>
                    <span class="pill">Job #: {sh.get('job_number')}</span>
                    <span class="pill">Vehicle: {sh.get('vehicle_name')} (#{sh.get('vehicle_barcode')})</span>
                    <span class="pill">Start: {sh.get('shift_start')}</span>
                    <span class="pill">End: {end_str}</span>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with col_edit:
            if st.button("Edit shift", key="edit_shift_card", use_container_width=True):
                st.session_state.view = "edit_shift"
                st.rerun()
    if sh.get("vehicle_location_mismatch"):
        st.warning(f"Vehicle location mismatch flagged. Expected: {sh.get('vehicle_location_expected')} · Actual: {sh.get('vehicle_location_actual')}")

    acts = storage.list_activities(st.session_state.shift_date, st.session_state.username)

    if st.session_state.get("view") == "well_report":
        target_id = st.session_state.get("well_report_activity_id") or st.session_state.get("edit_activity_id")
        target = next((a for a in acts if int(a.get("id")) == int(target_id)), None)
        if target:
            st.divider()
            well_report_view(target)
        else:
            st.warning("No activity found for well report.")
            st.session_state.view = "dd"
            st.session_state.well_report_activity_id = None
        return

    st.markdown("### Shift coverage")
    st.progress(shift_progress(sh, acts), text="Coverage of scheduled shift")
    st.caption("The bar fills as activities cover time inside the shift window.")
    st.markdown("#### Activity timeline")
    highlight_id = st.session_state.get("edit_activity_id") if st.session_state.get("view") == "edit_activity" else None
    activity_timeline(sh, acts, highlight_id=highlight_id)

    if st.session_state.get("view") == "edit_shift":
        st.divider()
        st.markdown("## Edit shift")
        shift_form(vehicles, site_options, existing=sh, form_key="shift_form_edit")

    if st.session_state.get("view") == "add_activity":
        st.divider()
        st.markdown("## Add activity")
        add_activity_form(catalog, sh, acts)
    if st.session_state.get("view") == "edit_activity":
        target = next((a for a in acts if a.get("id") == st.session_state.get("edit_activity_id")), None)
        if target:
            st.divider()
            st.markdown("## Edit activity")
            edit_activity_form(catalog, sh, acts, target)

    st.divider()
    hdr_l, hdr_r = st.columns([4, 1.3], vertical_alignment="center")
    with hdr_l:
        st.markdown("## Activities")
    with hdr_r:
        if st.button("Add activity", type="primary", use_container_width=True):
            st.session_state.view = "add_activity"
            st.session_state.edit_activity_id = None
            st.rerun()
    if not acts:
        st.info("No activities yet.")
        return

    # quick summary row
    total_minutes = 0
    for a in acts:
        try:
            a0 = datetime.fromisoformat(a.get("start_ts"))
            a1 = datetime.fromisoformat(a.get("end_ts"))
            total_minutes += max(0, int((a1 - a0).total_seconds() // 60))
        except Exception:
            continue
    st.caption(f"Total logged: {total_minutes/60:.2f} hours across {len(acts)} activities")

    for a in acts:
        try:
            a0 = datetime.fromisoformat(a.get("start_ts"))
            a1 = datetime.fromisoformat(a.get("end_ts"))
            duration_min = max(0, int((a1 - a0).total_seconds() // 60))
        except Exception:
            a0 = a1 = None
            duration_min = 0
        start_str = a0.strftime("%H:%M") if a0 else a.get("start_ts")
        end_str = a1.strftime("%H:%M") if a1 else a.get("end_ts")
        dur_str = f"{duration_min//60}h {duration_min%60:02d}m"
        code = a.get("code")
        code_color = CODE_COLORS.get(code, "#6c7a89")
        code_pill = f"<span class='pill' style='background:{code_color}; color:white; border:none;'>{code}</span>"
        is_editing = st.session_state.get("edit_activity_id") == a.get("id") and st.session_state.get("view") == "edit_activity"
        meta_bits = []
        if a.get("tool"):
            meta_bits.append(f"Tools: {a.get('tool')}")
        if a.get("hole_id"):
            meta_bits.append(f"Hole: {a.get('hole_id')}")
        if a.get("notes"):
            meta_bits.append("Notes")

        with st.container(border=True):
            if is_editing:
                st.markdown("<div style='background:rgba(255,210,77,0.15); padding:6px 8px; border-radius:10px;'>Editing this activity</div>", unsafe_allow_html=True)
            c_left, c_right = st.columns([6, 1], vertical_alignment="center")
            with c_left:
                st.markdown(f"{code_pill} <strong>{a.get('label')}</strong><br/><span class='muted'>{start_str} → {end_str} • {dur_str}</span>", unsafe_allow_html=True)
                if meta_bits:
                    st.caption(" · ".join(meta_bits))
                if a.get("notes"):
                    st.write(a.get("notes"))
        with c_right:
            if st.button("✏️", key=f"edit_{a.get('id')}", help="Edit activity", use_container_width=True):
                st.session_state.edit_activity_id = int(a.get("id"))
                st.session_state.view = "edit_activity"
                st.rerun()
            if st.button("🗑️", key=f"del_{a.get('id')}", help="Delete activity", use_container_width=True):
                storage.delete_activity(st.session_state.shift_date, st.session_state.username, int(a["id"]))
                st.rerun()

    st.divider()
    st.markdown(f"<div style='text-align:center; color:var(--wsg-muted);'>Version {VERSION}</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
