import os
import re
from pathlib import Path

import boto3
import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st


# ============================================================
# PAGE CONFIGURATION + THEME
# ============================================================

st.set_page_config(
    page_title="Mandi | Market Intelligence",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded",
)

BUCKET_NAME = "mandi-data-pipeline-vasudha"
AWS_REGION = "ap-south-1"

TABLE_FILES = {
    "gold.dim_date": "gold_dim_date.parquet",
    "gold.dim_market": "gold_dim_market.parquet",
    "gold.dim_commodity": "gold_dim_commodity.parquet",
    "gold.fact_market_prices": "gold_fact_market_prices.parquet",
    "ops.dq_results": "ops_dq_results.parquet",
    "ops.pipeline_runs": "ops_pipeline_runs.parquet",
    "ops.reconciliation": "ops_reconciliation.parquet",
    "silver.rejected_records": "silver_rejected_records.parquet",
}

# Forest green + warm ivory visual system
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Manrope:wght@500;600;700;800&display=swap');

    :root {
        --forest: #183D32;
        --forest-2: #245746;
        --sage: #DCE8DE;
        --mint: #EEF4EE;
        --gold: #C79A45;
        --ink: #1E2B25;
        --muted: #718078;
        --paper: #F7F8F3;
        --white: #FFFFFF;
        --line: #E3E9E1;
    }

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
        color: var(--ink);
    }

    .stApp {
        background: var(--paper);
    }

    [data-testid="stHeader"] {
        background: rgba(247, 248, 243, 0.92);
    }

    [data-testid="stSidebar"] {
        background: #EDF2EB;
        border-right: 1px solid #DCE5DC;
    }

    [data-testid="stSidebar"] > div:first-child {
        padding-top: 1.5rem;
    }

    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {
        color: var(--forest);
        font-family: 'Manrope', sans-serif;
    }

    h1, h2, h3 {
        font-family: 'Manrope', sans-serif !important;
        color: var(--forest) !important;
        letter-spacing: -0.035em;
    }

    h1 {
        font-size: 2.45rem !important;
        font-weight: 800 !important;
    }

    h2 {
        font-size: 1.55rem !important;
        font-weight: 750 !important;
    }

    h3 {
        font-size: 1.12rem !important;
        font-weight: 700 !important;
    }

    p, label, .stCaption {
        color: var(--muted);
    }

    [data-testid="stMetric"] {
        background: var(--white);
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 18px 20px;
        box-shadow: 0 3px 14px rgba(24, 61, 50, 0.045);
        min-height: 112px;
    }

    [data-testid="stMetricLabel"] {
        color: var(--muted);
        font-size: 0.82rem;
        font-weight: 600;
    }

    [data-testid="stMetricValue"] {
        color: var(--forest);
        font-family: 'Manrope', sans-serif;
        font-size: 1.8rem;
        font-weight: 800;
    }

    [data-testid="stMetricDelta"] {
        font-size: 0.78rem;
    }

    [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] {
        gap: 1rem;
    }

    div[data-testid="stPlotlyChart"] {
        background: var(--white);
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 10px 10px 2px 10px;
        box-shadow: 0 3px 14px rgba(24, 61, 50, 0.035);
    }

    [data-testid="stDataFrame"], [data-testid="stTable"] {
        background: var(--white);
        border: 1px solid var(--line);
        border-radius: 14px;
        overflow: hidden;
    }

    .hero {
        background: linear-gradient(115deg, #183D32 0%, #245746 68%, #39745A 100%);
        border-radius: 22px;
        padding: 30px 34px;
        color: white;
        margin: 4px 0 26px 0;
        box-shadow: 0 10px 28px rgba(24, 61, 50, 0.14);
        position: relative;
        overflow: hidden;
    }

    .hero:after {
        content: " ";
        position: absolute;
        width: 230px;
        height: 230px;
        border: 1px solid rgba(255,255,255,0.13);
        border-radius: 50%;
        right: -55px;
        top: -105px;
        box-shadow: 0 0 0 25px rgba(255,255,255,0.035),
                    0 0 0 55px rgba(255,255,255,0.025);
    }

    .hero-kicker {
        color: #D6E6D8;
        text-transform: uppercase;
        letter-spacing: 0.16em;
        font-size: 0.72rem;
        font-weight: 700;
        margin-bottom: 10px;
    }

    .hero-title {
        color: #FFFFFF !important;
        font-family: 'Manrope', sans-serif;
        font-size: 2.15rem;
        font-weight: 800;
        line-height: 1.15;
        margin: 0 0 9px 0;
        letter-spacing: -0.04em;
    }

    .hero-subtitle {
        color: #E0EAE1;
        font-size: 0.95rem;
        margin: 0;
    }

    .section-heading {
        display: flex;
        align-items: center;
        gap: 10px;
        margin: 25px 0 14px 0;
    }

    .section-dot {
        width: 9px;
        height: 9px;
        border-radius: 50%;
        background: var(--gold);
        display: inline-block;
    }

    .section-heading span:last-child {
        color: var(--forest);
        font-family: 'Manrope', sans-serif;
        font-size: 1.25rem;
        font-weight: 800;
        letter-spacing: -0.025em;
    }

    .section-note {
        color: var(--muted);
        font-size: 0.85rem;
        margin: -7px 0 15px 19px;
    }

    .sidebar-brand {
        background: var(--forest);
        color: white;
        border-radius: 15px;
        padding: 17px 16px;
        margin-bottom: 24px;
    }

    .sidebar-brand strong {
        color: white;
        font-family: 'Manrope', sans-serif;
        font-size: 1.08rem;
        display: block;
    }

    .sidebar-brand small {
        color: #D6E6D8;
        font-size: 0.76rem;
    }

    .status-pill {
        display: inline-block;
        background: #E1EFE3;
        color: #245746;
        border: 1px solid #C9DECD;
        padding: 5px 11px;
        border-radius: 99px;
        font-size: 0.75rem;
        font-weight: 700;
    }

    .footer {
        color: #87948B;
        font-size: 0.78rem;
        text-align: center;
        padding: 22px 0 10px 0;
    }

    .stButton > button, .stDownloadButton > button {
        border-radius: 10px;
        border: 1px solid #C9D8CC;
        color: var(--forest);
        background: white;
        font-weight: 650;
    }

    .stButton > button:hover, .stDownloadButton > button:hover {
        border-color: var(--forest-2);
        color: white;
        background: var(--forest-2);
    }

    hr {
        border-color: var(--line);
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# DATABASE HELPERS
# ============================================================

@st.cache_resource
def get_connection():
    data_dir = Path("data/dashboard_exports")
    data_dir.mkdir(parents=True, exist_ok=True)

    # Streamlit Cloud uses credentials stored in App Settings > Secrets.
    # Local development can fall back to the AWS CLI profile.
    try:
        aws = st.secrets["aws"]
    except (KeyError, FileNotFoundError):
        aws = None

    if aws is not None:
        try:
            access_key_id = aws["access_key_id"]
            secret_access_key = aws["secret_access_key"]
        except KeyError as exc:
            raise RuntimeError(
                "AWS secrets are incomplete. Add access_key_id and "
                "secret_access_key under [aws] in Streamlit Secrets."
            ) from exc

        if not access_key_id or not secret_access_key:
            raise RuntimeError(
                "AWS secrets are empty. Check access_key_id and "
                "secret_access_key under [aws] in Streamlit Secrets."
            )

        session = boto3.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=AWS_REGION,
        )
    else:
        credentials_file = Path.home() / ".aws" / "credentials"
        config_file = Path.home() / ".aws" / "config"

        if not credentials_file.exists() and not config_file.exists():
            raise RuntimeError(
                "AWS credentials were not found. In Streamlit Cloud, add "
                "[aws] access_key_id and secret_access_key in Manage app > "
                "Settings > Secrets. Locally, configure the "
                "'mandi-pipeline' AWS CLI profile."
            )

        session = boto3.Session(
            profile_name="mandi-pipeline",
            region_name=AWS_REGION,
        )

    s3 = session.client("s3")

    for filename in TABLE_FILES.values():
        local_path = data_dir / filename
        if not local_path.exists():
            s3.download_file(
                BUCKET_NAME,
                f"dashboard/{filename}",
                str(local_path),
            )

    connection = duckdb.connect(database=":memory:")

    for table, filename in TABLE_FILES.items():
        schema, table_name = table.split(".")
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        file_path = (data_dir / filename).resolve().as_posix()
        connection.execute(
            f"CREATE VIEW {schema}.{table_name} AS "
            f"SELECT * FROM read_parquet('{file_path}')"
        )

    return connection


@st.cache_data(ttl=300, show_spinner=False)
def run_query(query, params=None):
    connection = get_connection()
    duckdb_query = re.sub(
        r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)",
        r"$\1",
        query,
    )
    return connection.execute(duckdb_query, params or {}).df()


def get_options(query, params=None):
    df = run_query(query, params)
    if df.empty:
        return []
    return df.iloc[:, 0].dropna().tolist()


def fmt_number(value):
    return f"{int(value or 0):,}"


def section(title, note=None):
    st.markdown(
        f'<div class="section-heading"><span class="section-dot"></span>'
        f'<span>{title}</span></div>',
        unsafe_allow_html=True,
    )
    if note:
        st.markdown(
            f'<div class="section-note">{note}</div>',
            unsafe_allow_html=True,
        )


def style_chart(fig, height=390, show_legend=False):
    fig.update_layout(
        template="plotly_white",
        height=height,
        margin=dict(l=12, r=18, t=45, b=12),
        paper_bgcolor="rgba(255,255,255,0)",
        plot_bgcolor="rgba(255,255,255,0)",
        font=dict(family="DM Sans, sans-serif", color="#52635A", size=12),
        title=dict(
            font=dict(family="Manrope, sans-serif", size=15, color="#183D32"),
            x=0.02,
            xanchor="left",
        ),
        showlegend=show_legend,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        xaxis=dict(
            showgrid=True,
            gridcolor="#EDF1EC",
            zeroline=False,
            linecolor="#DCE5DC",
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            linecolor="#DCE5DC",
        ),
    )
    return fig


def build_filters(year, state, district, market, commodity):
    conditions = []
    params = {}

    if year != "All":
        conditions.append("d.year = :year")
        params["year"] = int(year)
    if state != "All":
        conditions.append("m.state = :state")
        params["state"] = state
    if district != "All":
        conditions.append("m.district = :district")
        params["district"] = district
    if market != "All":
        conditions.append("m.market = :market")
        params["market"] = market
    if commodity != "All":
        conditions.append("c.commodity = :commodity")
        params["commodity"] = commodity

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    return where_clause, params


# ============================================================
# SIDEBAR FILTERS
# ============================================================

st.sidebar.markdown(
    """
    <div class="sidebar-brand">
        <strong>MANDI INTELLIGENCE</strong>
        <small>Indian agricultural markets</small>
    </div>
    """,
    unsafe_allow_html=True,
)
st.sidebar.markdown("### Refine your view")
st.sidebar.caption("Filter the market data below.")

years = get_options("""
    SELECT DISTINCT year
    FROM gold.dim_date
    ORDER BY year DESC
""")
states = get_options("""
    SELECT DISTINCT state
    FROM gold.dim_market
    WHERE state IS NOT NULL
    ORDER BY state
""")
commodities = get_options("""
    SELECT DISTINCT commodity
    FROM gold.dim_commodity
    WHERE commodity IS NOT NULL
    ORDER BY commodity
""")

year = st.sidebar.selectbox("Year", ["All"] + years)
state = st.sidebar.selectbox("State", ["All"] + states)

district_query = """
    SELECT DISTINCT district
    FROM gold.dim_market
    WHERE district IS NOT NULL
"""
district_params = {}
if state != "All":
    district_query += " AND state = :state"
    district_params["state"] = state
district_query += " ORDER BY district"
districts = get_options(district_query, district_params)
district = st.sidebar.selectbox("District", ["All"] + districts)

market_query = """
    SELECT DISTINCT market
    FROM gold.dim_market
    WHERE market IS NOT NULL
"""
market_params = {}
if state != "All":
    market_query += " AND state = :state"
    market_params["state"] = state
if district != "All":
    market_query += " AND district = :district"
    market_params["district"] = district
market_query += " ORDER BY market"
markets = get_options(market_query, market_params)
market = st.sidebar.selectbox("Market", ["All"] + markets)
commodity = st.sidebar.selectbox("Commodity", ["All"] + commodities)

st.sidebar.divider()
if st.sidebar.button("↻  Refresh dashboard", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.caption("Data refreshes automatically every 5 minutes.")
where_clause, params = build_filters(year, state, district, market, commodity)


# ============================================================
# HERO HEADER
# ============================================================

st.markdown(
    """
    <div class="hero">
        <div class="hero-kicker">Market intelligence · India</div>
        <div class="hero-title">Indian Mandi Analytics</div>
        <p class="hero-subtitle">
            Agricultural price trends, commodity performance and data quality monitoring.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# MARKET OVERVIEW
# ============================================================

section("Market overview", "A snapshot of the selected market data.")

metrics_query = f"""
    SELECT
        COUNT(*) AS total_records,
        COUNT(DISTINCT m.market_key) AS total_markets,
        COUNT(DISTINCT c.commodity_key) AS total_commodities,
        ROUND(AVG(f.modal_price), 2) AS avg_modal_price,
        ROUND(MIN(f.modal_price), 2) AS min_modal_price,
        ROUND(MAX(f.modal_price), 2) AS max_modal_price
    FROM gold.fact_market_prices f
    JOIN gold.dim_date d ON f.date_key = d.date_key
    JOIN gold.dim_market m ON f.market_key = m.market_key
    JOIN gold.dim_commodity c ON f.commodity_key = c.commodity_key
    {where_clause}
"""
metrics = run_query(metrics_query, params).iloc[0]

# Select one internally consistent successful run.
# A pipeline run is preferred when its recorded rejected count matches the
# actual rejected_records rows. This avoids pairing KPIs from one run with
# DQ/rejection records from another run.
quality_query = """
    WITH rejected AS (
        SELECT run_id, COUNT(*) AS actual_rejected
        FROM silver.rejected_records
        GROUP BY run_id
    ),
    dq AS (
        SELECT run_id, COUNT(*) AS dq_checks
        FROM ops.dq_results
        GROUP BY run_id
    ),
    reconciliation AS (
        SELECT run_id, COUNT(*) AS reconciliation_checks
        FROM ops.reconciliation
        GROUP BY run_id
    )
    SELECT
        p.run_id,
        p.source_file,
        p.status,
        COALESCE(p.accepted_rows, 0) AS accepted,
        COALESCE(p.rejected_rows, 0) AS rejected,
        COALESCE(p.total_rows, 0) AS total,
        COALESCE(r.actual_rejected, 0) AS actual_rejected,
        COALESCE(d.dq_checks, 0) AS dq_checks,
        COALESCE(rc.reconciliation_checks, 0) AS reconciliation_checks
    FROM ops.pipeline_runs p
    LEFT JOIN rejected r ON r.run_id = p.run_id
    LEFT JOIN dq d ON d.run_id = p.run_id
    LEFT JOIN reconciliation rc ON rc.run_id = p.run_id
    WHERE p.status = 'SUCCESS'
    ORDER BY
        CASE
            WHEN COALESCE(p.rejected_rows, 0) = COALESCE(r.actual_rejected, 0)
            THEN 0 ELSE 1
        END,
        CASE WHEN COALESCE(d.dq_checks, 0) > 0 THEN 0 ELSE 1 END,
        CASE WHEN COALESCE(rc.reconciliation_checks, 0) > 0 THEN 0 ELSE 1 END,
        p.end_time DESC NULLS LAST,
        p.start_time DESC
    LIMIT 1
"""
quality_df = run_query(quality_query)

if quality_df.empty:
    latest_run_id = None
    latest_source_file = None
    accepted = rejected = total_processed = actual_rejected = 0
    dq_check_count = reconciliation_check_count = 0
else:
    quality = quality_df.iloc[0]
    latest_run_id = quality["run_id"]
    latest_source_file = quality["source_file"]
    accepted = int(quality["accepted"])
    rejected = int(quality["rejected"])
    total_processed = int(quality["total"])
    actual_rejected = int(quality["actual_rejected"])
    dq_check_count = int(quality["dq_checks"])
    reconciliation_check_count = int(quality["reconciliation_checks"])

acceptance_rate = accepted / total_processed * 100 if total_processed else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Price records", fmt_number(metrics["total_records"]))
k2.metric("Markets", fmt_number(metrics["total_markets"]))
k3.metric("Commodities", fmt_number(metrics["total_commodities"]))
k4.metric(
    "Average modal price",
    f"₹{metrics['avg_modal_price']:,.2f}"
    if pd.notna(metrics["avg_modal_price"]) else "N/A",
)

k5, k6, k7 = st.columns(3)
k5.metric("Accepted records", fmt_number(accepted))
k6.metric("Rejected records", fmt_number(rejected))
k7.metric("Acceptance rate", f"{acceptance_rate:.2f}%")

if latest_run_id:
    st.caption(
        f"Pipeline run used for quality metrics: {latest_run_id} "
        f"· {fmt_number(total_processed)} processed records"
    )
    if rejected != actual_rejected:
        st.warning(
            "The selected run's rejected_rows value does not match the rows "
            "currently stored in silver.rejected_records. Check run IDs before "
            "treating the rejection analysis as complete."
        )
else:
    st.warning("No successful pipeline run is available in ops.pipeline_runs.")

st.markdown("")


# ============================================================
# MARKET PRICE TRENDS
# ============================================================

section("Price movement", "Monthly average modal price across the selected markets.")

trend_query = f"""
    SELECT
        DATE_TRUNC('month', d.full_date)::date AS month,
        ROUND(AVG(f.modal_price), 2) AS avg_modal_price,
        ROUND(AVG(f.min_price), 2) AS avg_min_price,
        ROUND(AVG(f.max_price), 2) AS avg_max_price
    FROM gold.fact_market_prices f
    JOIN gold.dim_date d ON f.date_key = d.date_key
    JOIN gold.dim_market m ON f.market_key = m.market_key
    JOIN gold.dim_commodity c ON f.commodity_key = c.commodity_key
    {where_clause}
    GROUP BY DATE_TRUNC('month', d.full_date)
    ORDER BY month
"""
trend_df = run_query(trend_query, params)

if not trend_df.empty:
    fig = px.line(
        trend_df,
        x="month",
        y="avg_modal_price",
        markers=True,
        labels={"month": "Month", "avg_modal_price": "Average modal price (₹)"},
    )
    fig.update_traces(
        line=dict(color="#245746", width=3),
        marker=dict(color="#C79A45", size=8, line=dict(color="white", width=1.5)),
        hovertemplate="%{x|%b %Y}<br>₹%{y:,.2f}<extra></extra>",
    )
    fig = style_chart(fig, height=390)
    fig.update_layout(yaxis_title="Average price (₹)", xaxis_title="")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No price trend data found for these filters.")


# ============================================================
# COMMODITY ANALYSIS
# ============================================================

section("Commodity analysis", "Compare the most frequently reported commodities and their average modal prices.")

commodity_query = f"""
    SELECT
        c.commodity,
        ROUND(AVG(f.modal_price), 2) AS avg_modal_price,
        COUNT(*) AS records
    FROM gold.fact_market_prices f
    JOIN gold.dim_date d ON f.date_key = d.date_key
    JOIN gold.dim_market m ON f.market_key = m.market_key
    JOIN gold.dim_commodity c ON f.commodity_key = c.commodity_key
    {where_clause}
    GROUP BY c.commodity
    ORDER BY records DESC
    LIMIT 10
"""
commodity_df = run_query(commodity_query, params)

left, right = st.columns(2)
with left:
    st.markdown("**Most reported commodities**")
    if not commodity_df.empty:
        df_plot = commodity_df.sort_values("records")
        fig = px.bar(
            df_plot,
            x="records",
            y="commodity",
            orientation="h",
            text="records",
            labels={"records": "Records", "commodity": ""},
        )
        fig.update_traces(
            marker_color="#39745A",
            texttemplate="%{text:,.0f}",
            textposition="outside",
            cliponaxis=False,
        )
        fig = style_chart(fig, height=390)
        fig.update_layout(xaxis_title="Number of records", yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No commodity data found.")

with right:
    st.markdown("**Average modal price**")
    if not commodity_df.empty:
        df_plot = commodity_df.sort_values("avg_modal_price")
        fig = px.bar(
            df_plot,
            x="avg_modal_price",
            y="commodity",
            orientation="h",
            text="avg_modal_price",
            labels={"avg_modal_price": "Average price (₹)", "commodity": ""},
        )
        fig.update_traces(
            marker_color="#C79A45",
            texttemplate="₹%{text:,.0f}",
            textposition="outside",
            cliponaxis=False,
        )
        fig = style_chart(fig, height=390)
        fig.update_layout(xaxis_title="Average modal price (₹)", yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No commodity data found.")


# ============================================================
# MARKET ANALYSIS
# ============================================================

section("Market analysis", "Markets with the highest number of price observations.")

market_analysis_query = f"""
    SELECT
        m.state,
        m.district,
        m.market,
        COUNT(*) AS records,
        ROUND(AVG(f.modal_price), 2) AS avg_modal_price
    FROM gold.fact_market_prices f
    JOIN gold.dim_date d ON f.date_key = d.date_key
    JOIN gold.dim_market m ON f.market_key = m.market_key
    JOIN gold.dim_commodity c ON f.commodity_key = c.commodity_key
    {where_clause}
    GROUP BY m.state, m.district, m.market
    ORDER BY records DESC
    LIMIT 20
"""
market_df = run_query(market_analysis_query, params)
st.dataframe(market_df, use_container_width=True, hide_index=True)


# ============================================================
# REJECTION ANALYSIS
# ============================================================

section(
    "Data quality | Rejected records",
    "A clear breakdown of records that failed validation in the selected pipeline run."
)

rejection_query = """
    SELECT
        rejection_reason,
        COUNT(*) AS rejected_records,
        ROUND(
            COUNT(*) * 100.0 /
            NULLIF(SUM(COUNT(*)) OVER (), 0),
            2
        ) AS percentage
    FROM silver.rejected_records
    WHERE run_id = :run_id
    GROUP BY rejection_reason
    ORDER BY rejected_records DESC
"""
rejection_df = (
    run_query(rejection_query, {"run_id": latest_run_id})
    if latest_run_id
    else pd.DataFrame(columns=["rejection_reason", "rejected_records", "percentage"])
)

reason_labels = {
    "non_positive_min_price": "Minimum price is zero or negative",
    "non_positive_max_price": "Maximum price is zero or negative",
    "modal_above_max": "Modal price exceeds maximum price",
    "min_above_modal": "Minimum price exceeds modal price",
    "duplicate_identical_prices": "Duplicate record with identical prices",
    "conflicting_duplicate_prices": "Duplicate record with conflicting prices",
}

short_reason_labels = {
    "non_positive_min_price": "Invalid minimum price",
    "non_positive_max_price": "Invalid maximum price",
    "modal_above_max": "Modal price above maximum",
    "min_above_modal": "Minimum price above modal",
    "duplicate_identical_prices": "Duplicate: identical prices",
    "conflicting_duplicate_prices": "Duplicate: conflicting prices",
}


def readable_reason(reason):
    parts = str(reason).split(";")
    return " + ".join(
        reason_labels.get(part, part.replace("_", " ").title())
        for part in parts
    )


def short_reason(reason):
    parts = str(reason).split(";")
    labels = [
        short_reason_labels.get(part, part.replace("_", " ").title())
        for part in parts
    ]
    if len(labels) > 2:
        return "Multiple validation failures"
    return " + ".join(labels)


if not rejection_df.empty:
    rejection_df["display_reason"] = rejection_df["rejection_reason"].apply(readable_reason)
    rejection_df["chart_reason"] = rejection_df["rejection_reason"].apply(short_reason)
    total_rejected = int(rejection_df["rejected_records"].sum())
    rejection_share = (
        total_rejected / total_processed * 100
        if total_processed else 0
    )

    metric_cols = st.columns([1, 1, 1.4])
    metric_cols[0].metric("Rejected records", fmt_number(total_rejected))
    metric_cols[1].metric("Validation categories", fmt_number(len(rejection_df)))
    metric_cols[2].metric("Share of processed records", f"{rejection_share:.2f}%")

    st.markdown("#### Rejections by validation reason")
    chart_df = rejection_df.sort_values("rejected_records", ascending=True).copy()

    fig = px.bar(
        chart_df,
        x="rejected_records",
        y="chart_reason",
        orientation="h",
        text="rejected_records",
        custom_data=["display_reason", "percentage"],
        labels={
            "rejected_records": "Number of rejected records",
            "chart_reason": "",
        },
        color_discrete_sequence=["#B96B4B"],
    )
    fig.update_traces(
        marker=dict(
            color="#B96B4B",
            line=dict(color="#A85D40", width=0.5),
        ),
        texttemplate="%{text:,.0f}",
        textposition="outside",
        cliponaxis=False,
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>"
            "Rejected records: %{x:,.0f}<br>"
            "Share: %{customdata[1]:.2f}%"
            "<extra></extra>"
        ),
    )
    fig.update_layout(
        template="plotly_white",
        height=max(320, 72 * len(chart_df) + 100),
        margin=dict(l=18, r=65, t=18, b=55),
        paper_bgcolor="rgba(255,255,255,0)",
        plot_bgcolor="rgba(255,255,255,0)",
        font=dict(family="DM Sans, sans-serif", color="#52635A", size=12),
        showlegend=False,
        bargap=0.38,
        xaxis=dict(
            title="Number of rejected records",
            showgrid=True,
            gridcolor="#EDF1EC",
            zeroline=False,
            rangemode="tozero",
            tickformat=",",
            title_font=dict(size=12),
        ),
        yaxis=dict(
            title="",
            showgrid=False,
            automargin=True,
            tickfont=dict(size=12, color="#40564A"),
        ),
    )
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={"displayModeBar": False, "responsive": True},
    )

    summary_df = rejection_df[
        ["display_reason", "rejected_records", "percentage"]
    ].rename(
        columns={
            "display_reason": "Validation reason",
            "rejected_records": "Rejected records",
            "percentage": "Share (%)",
        }
    )
    summary_df["Rejected records"] = summary_df["Rejected records"].map(
        lambda value: f"{int(value):,}"
    )
    summary_df["Share (%)"] = summary_df["Share (%)"].map(
        lambda value: f"{float(value):.2f}%"
    )

    with st.expander("View rejection summary", expanded=True):
        st.dataframe(
            summary_df,
            use_container_width=True,
            hide_index=True,
            height=min(350, 42 * (len(summary_df) + 1) + 5),
        )
        st.download_button(
            "Download rejection summary",
            data=summary_df.to_csv(index=False).encode("utf-8"),
            file_name="mandi_rejection_summary.csv",
            mime="text/csv",
        )

    with st.expander("Inspect sample rejected records"):
        sample_query = """
            SELECT
                state,
                district,
                market,
                commodity,
                arrival_date,
                min_price,
                max_price,
                modal_price,
                rejection_reason
            FROM silver.rejected_records
            WHERE run_id = :run_id
            ORDER BY rejection_id
            LIMIT 100
        """
        sample_df = run_query(sample_query, {"run_id": latest_run_id})
        if not sample_df.empty:
            sample_df["rejection_reason"] = sample_df["rejection_reason"].apply(readable_reason)
            st.dataframe(sample_df, use_container_width=True, hide_index=True)
        else:
            st.info("No sample rows are available for this run.")
else:
    if rejected > 0:
        st.error(
            f"The selected run reports {fmt_number(rejected)} rejected records, "
            "but no matching rows were found in silver.rejected_records."
        )
    else:
        st.success("No rejected records found for the selected pipeline run.")


# ============================================================
# PIPELINE + DATA QUALITY MONITORING
# ============================================================

section("Pipeline monitoring", "Execution history and validation results from the warehouse.")

if latest_run_id:
    st.markdown(
        f'<span class="status-pill">Selected run: {latest_run_id}</span>',
        unsafe_allow_html=True,
    )
    st.caption(
        f"Source: {latest_source_file or 'Not recorded'}"
        f" · DQ checks: {dq_check_count}"
        f" · Reconciliation entries: {reconciliation_check_count}"
    )

dq_query = """
    SELECT
        run_id,
        rule_name,
        rows_checked,
        rows_failed,
        failure_percentage,
        checked_at
    FROM ops.dq_results
    WHERE run_id = :run_id
    ORDER BY checked_at DESC, rule_name
    LIMIT 50
"""
dq_df = (
    run_query(dq_query, {"run_id": latest_run_id})
    if latest_run_id
    else pd.DataFrame()
)

st.markdown("**Data quality checks**")
if not dq_df.empty:
    st.dataframe(dq_df, use_container_width=True, hide_index=True)
else:
    st.info("No data quality results available.")

runs_query = """
    SELECT
        run_id,
        source_file,
        status,
        total_rows,
        accepted_rows,
        rejected_rows,
        ROUND(duration_seconds::numeric, 2) AS duration_seconds,
        start_time,
        end_time
    FROM ops.pipeline_runs
    ORDER BY start_time DESC
    LIMIT 20
"""
runs_df = run_query(runs_query)

st.markdown("**Pipeline execution history**")
if not runs_df.empty:
    st.dataframe(runs_df, use_container_width=True, hide_index=True)
    status_counts = runs_df["status"].value_counts().reset_index()
    status_counts.columns = ["status", "runs"]
    fig = px.pie(
        status_counts,
        names="status",
        values="runs",
        hole=0.65,
        color="status",
        color_discrete_map={
            "SUCCESS": "#39745A",
            "FAILED": "#B96B4B",
            "RUNNING": "#C79A45",
        },
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig = style_chart(fig, height=330)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No pipeline execution history available.")

reconciliation_query = """
    SELECT
        run_id,
        layer_name,
        row_count,
        recorded_at
    FROM ops.reconciliation
    WHERE run_id = :run_id
    ORDER BY recorded_at DESC, layer_name
"""
reconciliation_df = (
    run_query(reconciliation_query, {"run_id": latest_run_id})
    if latest_run_id
    else pd.DataFrame()
)

st.markdown("**Warehouse reconciliation**")
if not reconciliation_df.empty:
    st.dataframe(reconciliation_df, use_container_width=True, hide_index=True)
else:
    st.info("No reconciliation records available.")


# ============================================================
# FOOTER
# ============================================================

st.markdown(
    """
    <div class="footer">
        MANDI DATA QUALITY PIPELINE&nbsp;&nbsp; · &nbsp;&nbsp;
        PostgreSQL&nbsp; + &nbsp;Python&nbsp; + &nbsp;Streamlit&nbsp; + &nbsp;Plotly
        <br>Historical mandi price dataset · Dashboard cache: 1 minute
    </div>
    """,
    unsafe_allow_html=True,
)
