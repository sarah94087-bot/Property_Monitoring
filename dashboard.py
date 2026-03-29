"""
Property Monitoring Dashboard — Streamlit UI

Run:
    streamlit run dashboard.py
"""

import io
import subprocess
import sys

import pandas as pd
import streamlit as st

from storage import get_all_cases, get_last_scraped, init_db

st.set_page_config(
    page_title="Property Monitoring Dashboard",
    page_icon="🏠",
    layout="wide",
)

st.markdown("""
<style>
/* ── Global ── */
[data-testid="stAppViewContainer"] { background: #f8f9fb; }
[data-testid="stSidebar"] { background: #1e2433; }
[data-testid="stSidebar"] * { color: #e2e8f0 !important; }
[data-testid="stSidebar"] .stButton button {
    background: #3b82f6 !important;
    color: white !important;
    border-radius: 8px !important;
    border: none !important;
    font-weight: 600 !important;
}
[data-testid="stSidebar"] .stTextInput input {
    background: #2d3748 !important;
    border: 1px solid #4a5568 !important;
    color: #e2e8f0 !important;
    border-radius: 6px !important;
}
[data-testid="stSidebar"] .stMultiSelect div {
    background: #2d3748 !important;
    border-color: #4a5568 !important;
}
/* ── Metric cards ── */
[data-testid="metric-container"] {
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 20px 24px;
    box-shadow: 0 1px 3px rgba(0,0,0,.06);
}
[data-testid="stMetricValue"] { font-size: 2rem !important; font-weight: 700 !important; }
/* ── Dataframe ── */
[data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; box-shadow: 0 1px 4px rgba(0,0,0,.08); }
/* ── Divider ── */
hr { border-color: #e2e8f0 !important; margin: 1.5rem 0 !important; }
/* ── Status badges ── */
.badge-open   { background:#fef3c7; color:#92400e; padding:2px 10px; border-radius:12px; font-size:12px; font-weight:600; }
.badge-closed { background:#d1fae5; color:#065f46; padding:2px 10px; border-radius:12px; font-size:12px; font-weight:600; }
.badge-high   { background:#fee2e2; color:#991b1b; padding:2px 10px; border-radius:12px; font-size:12px; font-weight:600; }
.badge-medium { background:#fef3c7; color:#92400e; padding:2px 10px; border-radius:12px; font-size:12px; font-weight:600; }
.badge-low    { background:#d1fae5; color:#065f46; padding:2px 10px; border-radius:12px; font-size:12px; font-weight:600; }
</style>
""", unsafe_allow_html=True)

init_db()

PRIORITY_BG = {"High": "#fee2e2", "Medium": "#fef9c3", "Low": "#dcfce7"}
PRIORITY_FG = {"High": "#991b1b", "Medium": "#854d0e", "Low": "#166534"}


# ── Helpers ────────────────────────────────────────────────────────────────────

def run_scraper(apn: str) -> bool:
    with st.spinner("Scraping live data..."):
        result = subprocess.run(
            [sys.executable, "scraper.py", "--apn", apn],
            capture_output=True, text=True,
        )
    if result.returncode == 0:
        st.success("Data refreshed successfully")
        return True
    st.error(f"Scrape failed:\n{result.stderr[-500:]}")
    return False


def format_df(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["🆕"] = d["is_new"].apply(lambda x: "NEW" if x else "")
    d["Priority"] = d["priority"].apply(
        lambda p: f"{'' if p == 'High' else '' if p == 'Medium' else ''} {p}"
    )
    d["Status"] = d["status"].apply(
        lambda s: f"{'🟡' if s == 'Open' else '🟢'} {s}"
    )
    col_map = {
        "case_number":    "Case #",
        "case_type":      "Type",
        "current_status": "Last Activity",
        "open_date":      "Opened",
        "close_date":     "Closed",
        "inspector":        "Inspector",
        "council_district": "District",
        "activity_count":   "Events",
    }
    d.rename(columns={k: v for k, v in col_map.items() if k in d.columns}, inplace=True)
    for col in ("Opened", "Closed"):
        if col in d.columns:
            d[col] = pd.to_datetime(d[col], errors="coerce").dt.strftime("%d/%m/%Y")
    preferred = ["🆕", "Case #", "Type", "Status", "Priority", "Last Activity", "Opened", "Closed", "Inspector", "District", "Events"]
    return d[[c for c in preferred if c in d.columns]]


def style_priority(val):
    p = val.strip().split()[-1] if val.strip() else ""
    bg = PRIORITY_BG.get(p, "")
    fg = PRIORITY_FG.get(p, "")
    return f"background-color:{bg}; color:{fg}; font-weight:600;" if bg else ""


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏠 Property Monitor")
    st.markdown("---")
    apn = st.text_input("APN", value="2654002037", label_visibility="visible")
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("⟳  Refresh Data", use_container_width=True, type="primary"):
        if run_scraper(apn):
            st.rerun()

    last = get_last_scraped()
    if last:
        st.caption(f"Last sync: {last.strftime('%d/%m/%Y  %H:%M')}")

    st.markdown("---")
    st.markdown("**Filters**")
    filter_status   = st.multiselect("Status",   ["Open", "Closed"],      default=[])
    filter_priority = st.multiselect("Priority", ["High", "Medium", "Low"], default=[])
    filter_new_only = st.checkbox("New cases only (last 7 days)")

# ── Load data ──────────────────────────────────────────────────────────────────
df = get_all_cases()

if df.empty:
    st.markdown("## Property Monitoring Dashboard")
    st.info("No data yet — click **Refresh Data** in the sidebar to run the first scrape.")
    st.stop()

# Dynamic case-type filter (needs data)
with st.sidebar:
    filter_type = st.multiselect(
        "Case Type",
        options=sorted(df["case_type"].dropna().unique()),
        default=[],
        key="type_filter",
    )

# ── Apply filters ──────────────────────────────────────────────────────────────
filtered = df[df["apn"] == apn].copy() if apn else df.copy()
if filter_status:
    filtered = filtered[filtered["status"].isin(filter_status)]
if filter_priority:
    filtered = filtered[filtered["priority"].isin(filter_priority)]
if filter_type:
    filtered = filtered[filtered["case_type"].isin(filter_type)]
if filter_new_only:
    filtered = filtered[filtered["is_new"]]

# ── Page header ────────────────────────────────────────────────────────────────
col_title, col_apn = st.columns([3, 1])
with col_title:
    st.markdown("## Property Monitoring Dashboard")
    st.caption("LA Housing Department — Code Enforcement Cases")
with col_apn:
    st.markdown(f"<div style='text-align:right;padding-top:12px;color:#6b7280;font-size:14px;'>APN: <strong>{apn}</strong></div>", unsafe_allow_html=True)

st.markdown("---")

# ── What Changed ───────────────────────────────────────────────────────────────
base_df = df[df["apn"] == apn].copy() if apn else df.copy()
changed = base_df[base_df["changed_at"].notna() & base_df["previous_status"].notna()]

if not changed.empty:
    st.markdown("### What Changed Since Last Scrape")
    for _, row in changed.iterrows():
        status_changed   = row["previous_status"] != row["status"]
        priority_changed = row["previous_priority"] != row["priority"]

        parts = []
        if status_changed:
            parts.append(f"Status: **{row['previous_status']}** → **{row['status']}**")
        if priority_changed:
            arrow = "⬆️" if row["priority"] == "High" else "⬇️"
            parts.append(f"Priority: **{row['previous_priority']}** → **{row['priority']}** {arrow}")

        if parts:
            changed_at = row["changed_at"].strftime("%d/%m/%Y %H:%M") if pd.notna(row["changed_at"]) else ""
            st.markdown(
                f"<div style='background:white;border-left:4px solid "
                f"{'#ef4444' if row['priority']=='High' else '#f59e0b' if row['priority']=='Medium' else '#22c55e'};"
                f"padding:10px 16px;border-radius:0 8px 8px 0;margin-bottom:8px;box-shadow:0 1px 3px rgba(0,0,0,.06);'>"
                f"<span style='font-weight:700;color:#1e2433;'>Case #{row['case_number']}</span>"
                f"<span style='color:#6b7280;font-size:12px;margin-left:12px;'>{row['case_type']}</span>"
                f"<span style='color:#9ca3af;font-size:11px;float:right;'>{changed_at}</span>"
                f"<br><span style='color:#374151;font-size:13px;'>{' &nbsp;|&nbsp; '.join(parts)}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )
    st.markdown("---")

# ── KPI cards ──────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Cases",     len(filtered))
c2.metric("Open",            len(filtered[filtered["status"] == "Open"]),
          delta="requires attention" if len(filtered[filtered["status"] == "Open"]) > 0 else None,
          delta_color="inverse")
c3.metric("🔴 High Priority", len(filtered[filtered["priority"] == "High"]))
c4.metric("🟡 Medium",        len(filtered[filtered["priority"] == "Medium"]))
c5.metric("🆕 New (7d)",      len(filtered[filtered["is_new"]]))

st.markdown("---")

# ── Main table ─────────────────────────────────────────────────────────────────
left, right = st.columns([4, 1])
with left:
    st.markdown(f"#### Cases &nbsp; <span style='color:#6b7280;font-size:16px;font-weight:400;'>({len(filtered)} results)</span>", unsafe_allow_html=True)
with right:
    sort_by = st.selectbox("Sort by", ["Priority", "Opened", "Case #"], label_visibility="collapsed")

if not filtered.empty:
    sort_col_map = {"Priority": "priority", "Opened": "open_date", "Case #": "case_number"}
    sorted_df = filtered.sort_values(sort_col_map[sort_by], ascending=(sort_by == "Case #"), na_position="last")

    display_df = format_df(sorted_df)
    st.dataframe(
        display_df.style.map(style_priority, subset=["Priority"]),
        use_container_width=True,
        hide_index=True,
        height=480,
        column_config={
            "🆕":           st.column_config.TextColumn("",           width=40),
            "Case #":       st.column_config.TextColumn("Case #",     width=90),
            "Type":         st.column_config.TextColumn("Type",       width=200),
            "Status":       st.column_config.TextColumn("Status",     width=100),
            "Priority":     st.column_config.TextColumn("Priority",   width=110),
            "Last Activity":st.column_config.TextColumn("Last Activity", width=260),
            "Opened":       st.column_config.TextColumn("Opened",     width=100),
            "Closed":       st.column_config.TextColumn("Closed",     width=100),
            "Events":       st.column_config.NumberColumn("Events",   width=70, format="%d"),
        },
    )
else:
    st.warning("No results match the current filters.")

st.markdown("---")

# ── Export ─────────────────────────────────────────────────────────────────────
def to_excel(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Cases")
        ws = writer.sheets["Cases"]
        for col in ws.columns:
            ws.column_dimensions[col[0].column_letter].width = max(
                len(str(col[0].value or "")),
                *(len(str(c.value or "")) for c in col[1:]),
                12,
            )
    return buf.getvalue()

if not filtered.empty:
    export_df = format_df(filtered)
    st.download_button(
        label="Export to Excel",
        data=to_excel(export_df),
        file_name=f"cases_{apn}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

st.markdown("---")

# ── Case detail search ─────────────────────────────────────────────────────────
with st.expander("🔍  Search case by number"):
    q = st.text_input("Enter case number", placeholder="e.g. 11412")
    if q:
        match = df[df["case_number"].str.contains(q, case=False, na=False)]
        if not match.empty:
            st.dataframe(format_df(match), use_container_width=True, hide_index=True)
        else:
            st.warning("No case found with that number.")

# ── Legend ─────────────────────────────────────────────────────────────────────
with st.expander("ℹ️  Priority logic"):
    st.markdown("""
| Priority | When |
|----------|------|
| 🔴 **High** | Hearing case (active legal process), OR open > 30 days |
| 🟡 **Medium** | Open 7–30 days, or open Complaint / Case Management |
| 🟢 **Low** | Closed, Training Program, or opened within the last week |
""")
