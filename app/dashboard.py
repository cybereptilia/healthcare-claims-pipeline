"""
dashboard.py

Purpose:
    Interactive dashboard for the healthcare claims pipeline. It reads the
    PostgreSQL views built by the pipeline and presents claims volume,
    payments, denials, and data quality with charts and filters.

Data sources (all created by run_all.ps1):
    - v_claims_base  (sql/012_dashboard_views.sql): one row per clean claim.
      Every chart that respects the sidebar filters reads from this view.
    - v_latest_run   (sql/012_dashboard_views.sql): the most recent SUCCESS run.

Prerequisites:
    1) Run the pipeline so the views exist:   .\\run_all.ps1 -SkipML
    2) Install packages:                      pip install -r requirements.txt
    3) Make the Postgres password available to this terminal session:
           $env:DB_PASSWORD = "your-password"
       (PGPASSWORD also works, so a terminal where run_all.ps1 already ran is fine.)

How to run (from repo root):
    streamlit run app/dashboard.py

Build status:
    Done:    connection, sidebar filters, Overview tab, Payments tab.
    Planned: Providers, Procedures & Diagnoses, and Data Quality tabs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine

# -----------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------

# Same environment variables and defaults as python/load_to_postgres.py,
# so the dashboard and the loader always point at the same database.
DB_NAME: str = os.getenv("DB_NAME", "healthcare_claims")
DB_USER: str = os.getenv("DB_USER", "postgres")
# DB_PASSWORD first; fall back to PGPASSWORD (the variable run_all.ps1 sets).
DB_PASSWORD: str = os.getenv("DB_PASSWORD") or os.getenv("PGPASSWORD", "")
DB_HOST: str = os.getenv("DB_HOST", "localhost")
DB_PORT: int = int(os.getenv("DB_PORT", "5432"))

# How long (seconds) query results are cached before Streamlit re-queries.
# Re-running the pipeline while the app is open shows up after this delay
# (or immediately with the "Clear cache" option in Streamlit's menu).
QUERY_CACHE_SECONDS: int = 600

# One accent color for single-series charts. Each chart shows ONE measure,
# so color carries no meaning by itself; titles and axis labels name what is
# shown. (Blue slot 1 of the reference palette; readable on light and dark.)
ACCENT_COLOR: str = "#2a78d6"

# Colors for charts that compare billed / allowed / paid on one shared $ axis.
# Fixed order (blue, orange, aqua = slots 1-3 of the reference palette), so
# each measure keeps its color in every chart. Light and dark mode use the
# same hues stepped for each background; both sets were checked with the
# palette validator (colorblind separation and contrast). In light mode the
# aqua line is below 3:1 contrast, so those charts also carry direct labels
# and a table view, and never rely on color alone.
AMOUNT_SERIES: list[str] = ["Billed", "Allowed", "Paid"]
AMOUNT_COLORS_LIGHT: list[str] = ["#2a78d6", "#eb6834", "#1baf7a"]
AMOUNT_COLORS_DARK: list[str] = ["#3987e5", "#d95926", "#199e70"]

# Note on dollar signs: Streamlit treats text between two "$" signs as a math
# formula in st.markdown / st.caption, so dollar amounts in those calls are
# written as "\\$" (an escaped dollar sign) to display normally.
# st.metric values are plain text and do not need escaping.


# -----------------------------------------------------------------
# Database access
# -----------------------------------------------------------------
@st.cache_resource
def get_engine() -> Engine:
    """
    Create ONE SQLAlchemy engine for the whole app session.

    Why cache_resource: an engine holds a pool of database connections.
    Streamlit re-runs this script top to bottom on every click, so without
    caching we would open a new pool on every interaction.

    Why URL.create: it escapes special characters in the password
    (for example "@" or "/"), which a hand-built URL string would break on.
    """
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASSWORD or None,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    )
    # pool_pre_ping checks a connection is alive before using it, so the app
    # recovers if Postgres was restarted while the dashboard was open.
    return create_engine(url, pool_pre_ping=True)


@st.cache_data(ttl=QUERY_CACHE_SECONDS, show_spinner=False)
def run_query(sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    """
    Run a read-only SQL query and return the result as a DataFrame.

    Why cache_data: the same query with the same filter values returns the
    same rows, so Streamlit can reuse the result instead of hitting the
    database on every click. The cache key is (sql, params).

    Values are passed as bind parameters (:name in the SQL), never pasted
    into the SQL string, so filter values can never change the query itself.
    """
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def check_connection() -> None:
    """
    Fail early with a readable message if the database or views are missing.
    Stops the app (st.stop) instead of showing a raw stack trace.
    """
    try:
        run_query("SELECT 1 FROM v_claims_base LIMIT 1")
    except Exception as exc:  # any connection / missing-view error
        st.error(
            "Could not read from the database.\n\n"
            "Check that:\n"
            "1. PostgreSQL is running.\n"
            "2. The pipeline has run, so the views exist: `.\\run_all.ps1 -SkipML`\n"
            "3. The password is set in this terminal: `$env:DB_PASSWORD = \"...\"`\n\n"
            f"Details: `{type(exc).__name__}: {str(exc).splitlines()[0]}`"
        )
        st.stop()


# -----------------------------------------------------------------
# Sidebar filters
# -----------------------------------------------------------------
def as_date(value: object) -> date:
    """ 
    Convert one value from a query result into a datetime.date.
    
    Why this helper: psycopg2 returns a PostgreSQL DATE as a Python date, but
    pandas types a single DataFrame cell as a generic "Scalar", so the type
    checker cannot know it is a date. Checking the type here makes that
    explicit and fails loudly if the column ever comes back as something else.
    
    datetime is checked first because datetime is a subclass of date.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raise TypeError(f"Expected a date, got {type(value).__name__}: {value!r}")


@dataclass(frozen=True)
class Filters:
    """
    The current sidebar selections.

    Tuples (not lists) so the values are hashable and stable, which keeps
    Streamlit's query cache working.
    """
    start_date: date
    end_date: date
    states: tuple[str, ...]
    statuses: tuple[str, ...]


def build_sidebar() -> Filters:
    """
    Draw the sidebar filters and return the current selections.
    The available options come from the data, so new states or statuses
    appear automatically after a pipeline re-run.
    """
    bounds = run_query(
        "SELECT MIN(service_date) AS min_date, MAX(service_date) AS max_date "
        "FROM v_claims_base"
    )
    min_date = as_date(bounds.loc[0, "min_date"])
    max_date = as_date(bounds.loc[0, "max_date"])

    states = run_query(
        "SELECT member_state, COUNT(*) AS n FROM v_claims_base "
        "GROUP BY member_state ORDER BY n DESC"
    )["member_state"].tolist()
    statuses = run_query(
        "SELECT status, COUNT(*) AS n FROM v_claims_base "
        "GROUP BY status ORDER BY n DESC"
    )["status"].tolist()

    st.sidebar.header("Filters")

    picked = st.sidebar.date_input(
        "Service date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    # While the user is picking, date_input returns only the start date.
    # Until both ends are chosen, keep the end of the full range.
    if isinstance(picked, tuple) and len(picked) == 2:
        start_date, end_date = picked
    elif isinstance(picked, tuple) and len(picked) == 1:
        start_date, end_date = picked[0], max_date
    else:
        start_date, end_date = min_date, max_date

    picked_states = st.sidebar.multiselect(
        "Member state", options=states, default=states
    )
    picked_statuses = st.sidebar.multiselect(
        "Claim status", options=statuses, default=statuses
    )

    st.sidebar.caption(
        "Filters apply to every chart built from clean claims. "
        "Data-quality figures describe the whole pipeline run and are not filtered."
    )

    return Filters(
        start_date=start_date,
        end_date=end_date,
        states=tuple(picked_states),
        statuses=tuple(picked_statuses),
    )


def filter_clause(filters: Filters) -> tuple[str, dict[str, Any]]:
    """
    Turn the sidebar selections into a SQL WHERE clause + bind parameters.

    Every filtered query uses this one function, so all charts always agree
    on which claims are included.

    "= ANY(:states)" compares against a list: psycopg2 sends a Python list
    as a PostgreSQL array.
    """
    where = (
        "WHERE service_date BETWEEN :start_date AND :end_date "
        "AND member_state = ANY(:states) "
        "AND status = ANY(:statuses)"
    )
    params: dict[str, Any] = {
        "start_date": filters.start_date,
        "end_date": filters.end_date,
        "states": list(filters.states),
        "statuses": list(filters.statuses),
    }
    return where, params


# -----------------------------------------------------------------
# Formatting and chart helpers
# -----------------------------------------------------------------
def format_money(amount: float) -> str:
    """
    Short money label for KPI tiles: $1.23M, $456.7K, $89.
    """
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:,.2f}M"
    if amount >= 1_000:
        return f"${amount / 1_000:,.1f}K"
    return f"${amount:,.0f}"


def amount_colors() -> list[str]:
    """
    Pick the light or dark set of billed/allowed/paid colors to match the
    viewer's Streamlit theme (st.context.theme reports "light" or "dark").
    Falls back to the light set if the theme is unknown.
    """
    theme_type = getattr(st.context.theme, "type", None)
    return AMOUNT_COLORS_DARK if theme_type == "dark" else AMOUNT_COLORS_LIGHT


# Column formats for st.dataframe tables, so every table in the app shows
# readable headers and consistent number formats instead of raw column names.
def money_column(label: str) -> Any:
    """Dollar amounts with thousands separators: $1,234.56"""
    return st.column_config.NumberColumn(label, format="dollar")


def count_column(label: str) -> Any:
    """Whole numbers with thousands separators: 17,323"""
    return st.column_config.NumberColumn(label, format="localized")


def pct_column(label: str) -> Any:
    """Percentages stored as 0-100 values (not 0-1): 13.09%"""
    return st.column_config.NumberColumn(label, format="%.2f%%")


def show_table(df: pd.DataFrame, columns: dict[str, Any]) -> None:
    """
    Show a DataFrame as a table with friendly headers and number formats.
    Columns not listed in `columns` keep their default display.
    """
    st.dataframe(df, hide_index=True, width="stretch", column_config=columns)


def month_axis() -> alt.Axis:
    """
    Shared x-axis for monthly charts: one tick per month, labeled "Oct 25".
    """
    return alt.Axis(
        format="%b %y",
        labelAngle=0,
        tickCount=alt.TimeIntervalStep(interval="month", step=1),
        grid=False,
    )


def single_line_chart(
    df: pd.DataFrame,
    value: str,
    title: str,
    value_title: str,
) -> alt.LayerChart:
    """
    One measure over time: a 2px line with a point on each month.

    Hover: a vertical hairline snaps to the nearest month and shows its
    tooltip, so the reader does not have to hit the small point exactly.
    Expects a "month" column of datetimes.
    """
    nearest = alt.selection_point(
        nearest=True, on="pointerover", fields=["month"], empty=False
    )
    base = alt.Chart(df).encode(x=alt.X("month:T", title=None, axis=month_axis()))
    y = alt.Y(f"{value}:Q", title=value_title, axis=alt.Axis(format="$,.0f", tickCount=5))

    line = base.mark_line(color=ACCENT_COLOR, strokeWidth=2).encode(y=y)
    points = base.mark_point(color=ACCENT_COLOR, filled=True, size=64).encode(y=y)
    # Invisible full-height rules catch the pointer; the hovered one is shown.
    hover_rule = (
        base.mark_rule(color="gray", strokeWidth=1)
        .encode(
            opacity=alt.condition(nearest, alt.value(0.6), alt.value(0)),
            tooltip=[
                alt.Tooltip("month:T", title="Month", format="%B %Y"),
                alt.Tooltip(f"{value}:Q", title=value_title, format="$,.2f"),
            ],
        )
        .add_params(nearest)
    )
    return (
        # alt.LayerChart(...) instead of alt.layer(...): same chart, but the
        # type checker then knows the result is a LayerChart.
        alt.LayerChart(layer=[line, points, hover_rule], title=title)
        .properties(height=280)
        .configure_axis(gridOpacity=0.3, domain=False, tickColor="transparent")
        .configure_view(stroke=None)
    )


def amounts_line_chart(df: pd.DataFrame, title: str) -> alt.LayerChart:
    """
    Billed, allowed, and paid per month as three lines on ONE shared $ axis.

    One axis is correct here because all three are the same unit (dollars);
    two different units would need two charts instead of a second y-axis.
    Identity is never color alone: there is a legend AND each line is
    labeled at its last month.

    Expects a "month" column plus total_billed / total_allowed / total_paid.
    """
    # Long format: one row per (month, measure) so Altair can color by measure.
    long_df = df.melt(
        id_vars="month",
        value_vars=["total_billed", "total_allowed", "total_paid"],
        var_name="measure",
        value_name="amount",
    )
    long_df["measure"] = long_df["measure"].map(
        {"total_billed": "Billed", "total_allowed": "Allowed", "total_paid": "Paid"}
    )

    color = alt.Color(
        "measure:N",
        scale=alt.Scale(domain=AMOUNT_SERIES, range=amount_colors()),
        legend=alt.Legend(title=None, orient="top"),
        sort=AMOUNT_SERIES,
    )
    x = alt.X("month:T", title=None, axis=month_axis())
    y = alt.Y("amount:Q", title="Amount ($)", axis=alt.Axis(format="$,.0f", tickCount=5))

    lines = alt.Chart(long_df).mark_line(strokeWidth=2).encode(x=x, y=y, color=color)
    points = alt.Chart(long_df).mark_point(filled=True, size=48).encode(x=x, y=y, color=color)

    # Direct labels: the measure name just right of each line's last point.
    # Text uses a neutral gray (readable on light and dark), not the line
    # color: the colored point right next to it already carries identity.
    last_month = long_df["month"].max()
    end_labels = (
        alt.Chart(long_df[long_df["month"] == last_month])
        .mark_text(align="left", dx=8, fontSize=12, color="#83858f")
        .encode(x=x, y=y, text="measure:N")
    )

    # One hover rule per month (from the wide table) shows all three values.
    nearest = alt.selection_point(
        nearest=True, on="pointerover", fields=["month"], empty=False
    )
    hover_rule = (
        alt.Chart(df)
        .mark_rule(color="gray", strokeWidth=1)
        .encode(
            x=x,
            opacity=alt.condition(nearest, alt.value(0.6), alt.value(0)),
            tooltip=[
                alt.Tooltip("month:T", title="Month", format="%B %Y"),
                alt.Tooltip("total_billed:Q", title="Billed", format="$,.2f"),
                alt.Tooltip("total_allowed:Q", title="Allowed", format="$,.2f"),
                alt.Tooltip("total_paid:Q", title="Paid", format="$,.2f"),
            ],
        )
        .add_params(nearest)
    )
    return (
        alt.LayerChart(layer=[lines, points, end_labels, hover_rule], title=title)
        .properties(height=320, padding={"right": 60})
        .configure_axis(gridOpacity=0.3, domain=False, tickColor="transparent")
        .configure_view(stroke=None)
    )


def bar_chart(
    df: pd.DataFrame,
    category: str,
    value: str,
    title: str,
    value_title: str,
    tooltip: list[alt.Tooltip],
) -> alt.Chart:
    """
    Horizontal single-series bar chart, sorted largest first.

    Why horizontal: category names (statuses, states, reasons) stay readable
    without rotated labels. Mark details follow the project's chart rules:
    thin bars with a gap between them, rounded data-end, recessive grid,
    and a hover tooltip on every bar.
    """
    return (
        alt.Chart(df, title=title)
        .mark_bar(color=ACCENT_COLOR, cornerRadiusEnd=4, size=18)
        .encode(
            x=alt.X(f"{value}:Q", title=value_title, axis=alt.Axis(grid=True, tickCount=5)),
            y=alt.Y(f"{category}:N", sort="-x", title=None, axis=alt.Axis(labelLimit=250)),
            tooltip=tooltip,
        )
        .properties(height=alt.Step(28))
        .configure_axis(gridOpacity=0.3, domain=False, tickColor="transparent")
        .configure_view(stroke=None)
    )


# -----------------------------------------------------------------
# Tab: Overview
# -----------------------------------------------------------------
def render_overview(filters: Filters) -> None:
    """
    Headline numbers for the filtered claims, plus status and state mix.
    """
    where, params = filter_clause(filters)

    # ---- KPI row -------------------------------------------------------------
    kpis = run_query(
        f"""
        SELECT
            COUNT(*)                         AS n_claims,
            COALESCE(SUM(billed_amount), 0)  AS total_billed,
            COALESCE(SUM(allowed_amount), 0) AS total_allowed,
            COALESCE(SUM(paid_amount), 0)    AS total_paid,
            -- AVG of a 0/1 column = share of denied claims
            COALESCE(AVG(is_denied), 0)      AS denial_rate
        FROM v_claims_base
        {where}
        """,
        params,
    ).iloc[0]

    n_claims = int(kpis["n_claims"])
    if n_claims == 0:
        st.warning("No claims match the current filters.")
        return

    total_billed = float(kpis["total_billed"])
    total_paid = float(kpis["total_paid"])

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Claims", f"{n_claims:,}")
    col2.metric("Billed", format_money(total_billed))
    col3.metric("Allowed", format_money(float(kpis["total_allowed"])))
    col4.metric("Paid", format_money(total_paid))
    col5.metric("Denial rate", f"{float(kpis['denial_rate']) * 100:.2f}%")

    st.caption(
        f"Paid is {total_paid / total_billed * 100:.1f}% of billed for the selected claims."
        if total_billed > 0
        else ""
    )

    # ---- Status mix and member state -------------------------------------------
    left, right = st.columns(2)

    status_mix = run_query(
        f"""
        SELECT
            status,
            COUNT(*) AS n_claims,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_claims
        FROM v_claims_base
        {where}
        GROUP BY status
        """,
        params,
    )
    with left:
        st.altair_chart(
            bar_chart(
                status_mix,
                category="status",
                value="n_claims",
                title="Claims by status",
                value_title="Claims",
                tooltip=[
                    alt.Tooltip("status:N", title="Status"),
                    alt.Tooltip("n_claims:Q", title="Claims", format=","),
                    alt.Tooltip("pct_of_claims:Q", title="% of claims", format=".2f"),
                ],
            ),
            width="stretch",
        )

    state_mix = run_query(
        f"""
        SELECT
            member_state,
            COUNT(*) AS n_claims,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_claims,
            SUM(paid_amount) AS total_paid
        FROM v_claims_base
        {where}
        GROUP BY member_state
        """,
        params,
    )
    with right:
        st.altair_chart(
            bar_chart(
                state_mix,
                category="member_state",
                value="n_claims",
                title="Claims by member state",
                value_title="Claims",
                tooltip=[
                    alt.Tooltip("member_state:N", title="State"),
                    alt.Tooltip("n_claims:Q", title="Claims", format=","),
                    alt.Tooltip("pct_of_claims:Q", title="% of claims", format=".2f"),
                    alt.Tooltip("total_paid:Q", title="Paid ($)", format=",.2f"),
                ],
            ),
            width="stretch",
        )

    # ---- Table view (accessibility: the same numbers without the chart) ---------
    with st.expander("Show the numbers as a table"):
        show_table(
            status_mix.sort_values("n_claims", ascending=False),
            {
                "status": st.column_config.TextColumn("Status"),
                "n_claims": count_column("Claims"),
                "pct_of_claims": pct_column("% of claims"),
            },
        )
        show_table(
            state_mix.sort_values("n_claims", ascending=False),
            {
                "member_state": st.column_config.TextColumn("Member state"),
                "n_claims": count_column("Claims"),
                "pct_of_claims": pct_column("% of claims"),
                "total_paid": money_column("Paid"),
            },
        )


# -----------------------------------------------------------------
# Tab: Payments
# -----------------------------------------------------------------
def render_payments(filters: Filters) -> None:
    """
    Money over time for the filtered claims: paid by month, and billed vs.
    allowed vs. paid by month, plus the monthly table behind both charts.
    """
    where, params = filter_clause(filters)

    monthly = run_query(
        f"""
        SELECT
            service_month                           AS month,
            COUNT(*)                                AS n_claims,
            COUNT(*) FILTER (WHERE status = 'PAID') AS n_paid_claims,
            SUM(billed_amount)                      AS total_billed,
            SUM(allowed_amount)                     AS total_allowed,
            SUM(paid_amount)                        AS total_paid
        FROM v_claims_base
        {where}
        GROUP BY service_month
        ORDER BY service_month
        """,
        params,
    )
    if monthly.empty:
        st.warning("No claims match the current filters.")
        return

    # psycopg2 returns DATE values as Python dates; Altair needs datetimes.
    monthly["month"] = pd.to_datetime(monthly["month"])
    # NUMERIC sums arrive as Decimal; convert to float for charts and math.
    for col in ["total_billed", "total_allowed", "total_paid"]:
        monthly[col] = monthly[col].astype(float)

    total_paid = float(monthly["total_paid"].sum())
    total_allowed = float(monthly["total_allowed"].sum())
    n_paid_claims = int(monthly["n_paid_claims"].sum())
    n_months = len(monthly)

    # ---- KPI row -------------------------------------------------------------
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Paid", format_money(total_paid))
    col2.metric("Average paid per month", format_money(total_paid / n_months))
    col3.metric(
        "Average paid per PAID claim",
        format_money(total_paid / n_paid_claims) if n_paid_claims else "n/a",
    )
    col4.metric(
        "Paid ÷ allowed",
        f"{total_paid / total_allowed * 100:.1f}%" if total_allowed else "n/a",
    )
    st.caption(
        "Only PAID claims have a paid amount; DENIED, PENDED, and REVERSED claims "
        "count toward billed and allowed but are paid \\$0."
    )

    # Partial-month warning: a date filter that starts or ends mid-month makes
    # that month's totals look low even if nothing changed in the business.
    month_end = (pd.Timestamp(filters.end_date) + pd.offsets.MonthEnd(0)).date()
    if filters.start_date.day != 1 or filters.end_date != month_end:
        st.info(
            "The date filter starts or ends in the middle of a month, so the "
            "first or last month only includes part of that month."
        )

    # ---- Charts ------------------------------------------------------------------
    st.altair_chart(
        single_line_chart(
            monthly, value="total_paid", title="Paid by month", value_title="Paid"
        ),
        width="stretch",
    )
    st.altair_chart(
        amounts_line_chart(monthly, title="Billed, allowed, and paid by month"),
        width="stretch",
    )

    # ---- Table view ------------------------------------------------------------
    with st.expander("Show the numbers as a table"):
        table = monthly.copy()
        table["paid_pct_of_allowed"] = (
            100 * table["total_paid"] / table["total_allowed"]
        ).round(2)
        show_table(
            table[
                ["month", "n_claims", "total_billed", "total_allowed",
                 "total_paid", "paid_pct_of_allowed"]
            ],
            {
                "month": st.column_config.DateColumn("Month", format="MMM YYYY"),
                "n_claims": count_column("Claims"),
                "total_billed": money_column("Billed"),
                "total_allowed": money_column("Allowed"),
                "total_paid": money_column("Paid"),
                "paid_pct_of_allowed": pct_column("Paid ÷ allowed"),
            },
        )


# -----------------------------------------------------------------
# Page header: which pipeline run is on screen
# -----------------------------------------------------------------
def render_run_banner() -> None:
    """
    One line under the title naming the pipeline run the data came from.
    Not filtered: it describes the whole load.
    """
    run = run_query(
        # TO_CHAR formats the time in Postgres's own time zone (local time),
        # instead of pandas converting it to UTC.
        "SELECT TO_CHAR(finished_at, 'YYYY-MM-DD HH24:MI') AS finished_label, "
        "raw_rows, clean_rows, rejected_rows, reject_rate_pct "
        "FROM v_latest_run"
    )
    if run.empty:
        st.info("No successful pipeline run found. Run `.\\run_all.ps1 -SkipML` first.")
        return

    r = run.iloc[0]
    finished = r["finished_label"]
    st.caption(
        f"Data from the pipeline run finished {finished}: "
        f"{int(r['raw_rows']):,} raw claims → {int(r['clean_rows']):,} clean, "
        f"{int(r['rejected_rows']):,} rejected ({float(r['reject_rate_pct']):.2f}%)."
    )


# -----------------------------------------------------------------
# Main
# -----------------------------------------------------------------
def main() -> None:
    """
    Page layout: title, run banner, sidebar filters, then one tab per topic.
    """
    st.set_page_config(page_title="Healthcare Claims Dashboard", layout="wide")
    st.title("Healthcare Claims Dashboard")

    check_connection()
    render_run_banner()
    filters = build_sidebar()

    # Stop early if a multiselect was emptied: there is nothing to show,
    # and "= ANY(empty array)" would match no rows anyway.
    if not filters.states or not filters.statuses:
        st.warning("Select at least one member state and one claim status in the sidebar.")
        st.stop()

    overview_tab, payments_tab = st.tabs(["Overview", "Payments"])

    with overview_tab:
        render_overview(filters)
    with payments_tab:
        render_payments(filters)


if __name__ == "__main__":
    main()
