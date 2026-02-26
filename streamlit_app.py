"""
DataFoundation: Streamlit Monitoring & Analytics Dashboard
─────────────────────────────────────────────────────────
A rich, interactive dashboard for monitoring the ETL pipeline
and visualizing retail analytics from the data warehouse.

Run with: streamlit run streamlit_app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
import logging
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import GCP_PROJECT_ID, BQ_DATASET

# ═══════════════════════════════════════════════════════════════════════
# PAGE CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="DataFoundation | Retail Data Hub",
    page_icon="🏪",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ═══════════════════════════════════════════════════════════════════════
# CUSTOM STYLING
# ═══════════════════════════════════════════════════════════════════════

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Main background and font */
    .stApp {
        font-family: 'Inter', sans-serif;
    }
    
    /* Metric cards with glassmorphism and animation */
    div[data-testid="metric-container"] {
        background: rgba(30, 30, 46, 0.6) !important;
        backdrop-filter: blur(12px);
        border: 1px solid rgba(139, 92, 246, 0.2) !important;
        border-radius: 20px !important;
        padding: 24px !important;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3) !important;
        transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275) !important;
        animation: fadeInScale 0.8s ease-out forwards;
        overflow: hidden;
        position: relative;
    }
    
    /* Shimmer sweep effect */
    @keyframes shimmerSweep {
        0% { transform: translateX(-150%) skewX(-20deg); }
        100% { transform: translateX(150%) skewX(-20deg); }
    }
    
    div[data-testid="metric-container"]::after {
        content: "";
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: linear-gradient(
            90deg,
            transparent,
            rgba(139, 92, 246, 0.05),
            rgba(255, 255, 255, 0.05),
            rgba(139, 92, 246, 0.05),
            transparent
        );
        animation: shimmerSweep 4s infinite linear;
        pointer-events: none;
    }
    
    div[data-testid="metric-container"]::before {
        content: "";
        position: absolute;
        top: -50%;
        left: -50%;
        width: 200%;
        height: 200%;
        background: radial-gradient(circle, rgba(139, 92, 246, 0.1) 0%, transparent 70%);
        opacity: 0;
        transition: opacity 0.4s ease;
    }

    div[data-testid="metric-container"]:hover {
        transform: translateY(-8px) scale(1.03) !important;
        border-color: rgba(139, 92, 246, 0.6) !important;
        box-shadow: 0 15px 45px rgba(139, 92, 246, 0.25) !important;
        background: rgba(45, 45, 68, 0.8) !important;
    }
    
    div[data-testid="metric-container"]:hover::before {
        opacity: 1;
    }

    div[data-testid="metric-container"] label {
        color: #b0b0d0 !important;
        font-size: 0.9rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        margin-bottom: 8px !important;
    }
    
    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        color: #ffffff !important;
        font-weight: 800 !important;
        font-size: 2rem !important;
        transition: all 0.3s ease;
    }

    div[data-testid="metric-container"]:hover div[data-testid="stMetricValue"] {
        text-shadow: 0 0 15px rgba(139, 92, 246, 0.8);
        transform: scale(1.05);
    }

    @keyframes fadeInScale {
        from {
            opacity: 0;
            transform: scale(0.9) translateY(20px);
        }
        to {
            opacity: 1;
            transform: scale(1) translateY(0);
        }
    }

    
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f0f1a 0%, #1a1a2e 100%);
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #e0e0ff !important;
        font-weight: 200 !important;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 12px;
        padding: 10px 24px;
        font-weight: 600;
    }
    
    /* Custom header banner with animated gradient */
    .header-banner {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%);
        background-size: 200% 200%;
        animation: gradientBG 15s ease infinite;
        padding: 40px;
        border-radius: 24px;
        margin-bottom: 32px;
        text-align: center;
        box-shadow: 0 10px 40px rgba(102, 126, 234, 0.4);
        position: relative;
        overflow: hidden;
    }
    
    @keyframes gradientBG {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    
    .header-banner::before {
        content: "";
        position: absolute;
        top: 0; left: 0; right: 0; bottom: 0;
        background: url('https://www.transparenttextures.com/patterns/cubes.png');
        opacity: 0.1;
    }
    
    .header-banner h1 {
        color: white !important;
        font-size: 2.5rem !important;
        font-weight: 800 !important;
        margin-bottom: 12px !important;
        text-shadow: 0 4px 10px rgba(0,0,0,0.2);
    }
    
    .header-banner p {
        color: rgba(255, 255, 255, 0.9);
        font-size: 1.1rem;
        margin: 0;
        font-weight: 400;
        letter-spacing: 1px;
    }
    
    /* Pipeline status badge */
    .status-badge {
        display: inline-block;
        padding: 6px 16px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.85rem;
    }
    
    .status-success {
        background: rgba(34, 197, 94, 0.2);
        color: #22c55e;
        border: 1px solid rgba(34, 197, 94, 0.4);
    }
    
    .status-running {
        background: rgba(59, 130, 246, 0.2);
        color: #3b82f6;
        border: 1px solid rgba(59, 130, 246, 0.4);
    }
    
    /* Data table styling */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
    }
    
    /* Info cards */
    .info-card {
        background: linear-gradient(135deg, #1e1e2e 0%, #252540 100%);
        border: 1px solid rgba(139, 92, 246, 0.2);
        border-radius: 16px;
        padding: 24px;
        margin-bottom: 16px;
    }
    
    /* Divider with glow */
    hr {
        border: none !important;
        height: 1px !important;
        background: linear-gradient(90deg, transparent, rgba(139, 92, 246, 0.5), transparent) !important;
        margin: 2rem 0 !important;
    }

    /* Staggered entry for metrics */
    div[data-testid="column"]:nth-of-type(1) div[data-testid="metric-container"] { animation-delay: 0.1s; }
    div[data-testid="column"]:nth-of-type(2) div[data-testid="metric-container"] { animation-delay: 0.15s; }
    div[data-testid="column"]:nth-of-type(3) div[data-testid="metric-container"] { animation-delay: 0.2s; }
    div[data-testid="column"]:nth-of-type(4) div[data-testid="metric-container"] { animation-delay: 0.25s; }
    div[data-testid="column"]:nth-of-type(5) div[data-testid="metric-container"] { animation-delay: 0.3s; }
    div[data-testid="column"]:nth-of-type(6) div[data-testid="metric-container"] { animation-delay: 0.35s; }

    /* ── Chart container animations ────────────────────────────────── */
    /* Slide-up + fade-in entrance for every Plotly iframe */
    div[data-testid="stPlotlyChart"] {
        animation: chartSlideUp 0.75s cubic-bezier(0.22, 1, 0.36, 1) both;
        border-radius: 18px;
        overflow: hidden;
        transition: transform 0.35s cubic-bezier(0.22, 1, 0.36, 1),
                    box-shadow 0.35s ease;
        position: relative;
    }

    div[data-testid="stPlotlyChart"]:hover {
        transform: translateY(-5px) scale(1.005);
        box-shadow: 0 20px 60px rgba(139, 92, 246, 0.22),
                    0 0 0 1px rgba(139, 92, 246, 0.18);
    }

    @keyframes chartSlideUp {
        0%  { opacity: 0; transform: translateY(40px) scale(0.97); }
        60% { opacity: 1; }
        100% { opacity: 1; transform: translateY(0)   scale(1);    }
    }

    /* Staggered chart delays per column position */
    div[data-testid="column"]:nth-of-type(1) div[data-testid="stPlotlyChart"] { animation-delay: 0.05s; }
    div[data-testid="column"]:nth-of-type(2) div[data-testid="stPlotlyChart"] { animation-delay: 0.15s; }
    div[data-testid="column"]:nth-of-type(3) div[data-testid="stPlotlyChart"] { animation-delay: 0.25s; }

    /* Animated glowing border on chart hover */
    div[data-testid="stPlotlyChart"]::after {
        content: "";
        position: absolute;
        inset: 0;
        border-radius: 18px;
        border: 1.5px solid rgba(139, 92, 246, 0);
        transition: border-color 0.35s ease,
                    box-shadow 0.35s ease;
        pointer-events: none;
    }
    div[data-testid="stPlotlyChart"]:hover::after {
        border-color: rgba(139, 92, 246, 0.45);
        box-shadow: inset 0 0 30px rgba(139, 92, 246, 0.07);
    }

    /* Section heading animation */
    h4, h3 {
        animation: headingFadeIn 0.6s ease both;
    }
    @keyframes headingFadeIn {
        from { opacity: 0; transform: translateX(-12px); }
        to   { opacity: 1; transform: translateX(0); }
    }

    /* Expander animation */
    div[data-testid="stExpander"] {
        animation: fadeInScale 0.5s ease both;
        border: 1px solid rgba(139, 92, 246, 0.15) !important;
        border-radius: 14px !important;
        transition: border-color 0.3s ease, box-shadow 0.3s ease;
    }
    div[data-testid="stExpander"]:hover {
        border-color: rgba(139, 92, 246, 0.4) !important;
        box-shadow: 0 6px 24px rgba(139, 92, 246, 0.12);
    }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════
# DATA LOADING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300)
def load_bigquery_data(table_name: str) -> pd.DataFrame:
    """Load data from BigQuery table with caching."""
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=GCP_PROJECT_ID)
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"
        query = f"SELECT * FROM `{table_id}`"
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.warning(f"⚠️ Could not load BigQuery data for `{table_name}`: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_local_csv() -> pd.DataFrame:
    """Load the local retail sales CSV for preview."""
    try:
        csv_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'retail_sales_dataset.csv'
        )
        return pd.read_csv(csv_path, parse_dates=['Date'])
    except Exception as e:
        st.warning(f"⚠️ Could not load CSV: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_api_products() -> pd.DataFrame:
    """Fetch products from Fake Store API for preview."""
    try:
        import requests
        resp = requests.get("https://fakestoreapi.com/products", timeout=15)
        resp.raise_for_status()
        products = resp.json()
        for p in products:
            rating = p.pop('rating', {})
            p['rating_rate'] = rating.get('rate', 0)
            p['rating_count'] = rating.get('count', 0)
        return pd.DataFrame(products)
    except Exception as e:
        st.warning(f"⚠️ Could not load API data: {e}")
        return pd.DataFrame()


def run_etl_pipeline(skip_load: bool = False):
    """Run the ETL pipeline from the dashboard."""
    from etl.pipeline import run_pipeline
    return run_pipeline(skip_load=skip_load)


# ═══════════════════════════════════════════════════════════════════════
# CHART THEME
# ═══════════════════════════════════════════════════════════════════════

CHART_COLORS = [
    '#8b5cf6', '#06b6d4', '#f59e0b', '#22c55e', '#ef4444',
    '#ec4899', '#3b82f6', '#14b8a6', '#f97316', '#a855f7'
]

CHART_TEMPLATE = {
    'layout': {
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'font': {'color': '#a0a0b8', 'family': 'Inter'},
        'xaxis': {
            'gridcolor': 'rgba(139, 92, 246, 0.1)',
            'zerolinecolor': 'rgba(139, 92, 246, 0.2)',
        },
        'yaxis': {
            'gridcolor': 'rgba(139, 92, 246, 0.1)',
            'zerolinecolor': 'rgba(139, 92, 246, 0.2)',
        },
        'margin': {'t': 40, 'b': 40, 'l': 40, 'r': 20},
    }
}


def apply_chart_animation(fig, duration: int = 800):
    """Apply smooth Plotly entrance transitions, animated hoverlabels, and
    interactive drag/zoom transitions to any Plotly figure."""
    fig.update_layout(
        # Plotly-native transitions (affect updates / re-renders)
        transition={
            'duration': duration,
            'easing': 'cubic-in-out',
        },
        # Rich hover label styling
        hoverlabel=dict(
            bgcolor='rgba(20, 16, 36, 0.92)',
            bordercolor='rgba(139, 92, 246, 0.7)',
            font=dict(
                family='Inter',
                size=13,
                color='#e0e0ff',
            ),
            namelength=-1,
        ),
        # Drag / selection transitions
        dragmode='zoom',
        # Subtle active-selection highlight colour
        newselection_line_color='rgba(139, 92, 246, 0.8)',
        activeselection_fillcolor='rgba(139, 92, 246, 0.08)',
    )
    # Animate every trace's marker on hover (works for scatter / bar)
    for trace in fig.data:
        trace_type = type(trace).__name__.lower()
        if 'scatter' in trace_type or 'bar' in trace_type:
            if hasattr(trace, 'marker') and trace.marker is not None:
                try:
                    trace.update(
                        selected=dict(
                            marker=dict(
                                opacity=1.0,
                                size=getattr(trace.marker, 'size', 10) or 10,
                            )
                        ),
                        unselected=dict(
                            marker=dict(opacity=0.45)
                        ),
                    )
                except Exception:
                    pass
    return fig


# ═══════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("### 🏪 DataFoundation")
    st.markdown("*Multi-source Retail Data Integration Hub*")
    st.divider()
    
    page = st.radio(
        "📑 Navigation",
        [
            "🏠 Dashboard Overview",
            # "📂 Data Sources",
            # "🔄 ETL Pipeline",
            "📊 Sales Analytics",
            "🏷️ Category Analysis",
            "👤 Customer Insights",
            "📦 Product Catalog",
            # "🗄️ Data Warehouse",
        ],
        label_visibility="collapsed",
    )
    
    # st.divider()
    
    # st.markdown("#### ⚙️ Settings")
    # data_source = st.selectbox(
    #     "Data Source",
    #     ["Local CSV + API (Preview)"]
    # )
    
    # auto_refresh = st.checkbox("Auto-refresh (5 min)", value=False)
    
    # st.divider()
    # st.markdown(
    #     f"<p style='color: #666; font-size: 0.75rem;'>"
    #     f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>",
    #     unsafe_allow_html=True
    # )


# ═══════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════

# Always load local data for preview
df_sales_raw = load_local_csv()
df_api_products = load_api_products()

# Load BigQuery data if selected
# use_bq = data_source == "BigQuery (Live)"


# ═══════════════════════════════════════════════════════════════════════
# PAGE: DASHBOARD OVERVIEW
# ═══════════════════════════════════════════════════════════════════════

if page == "🏠 Dashboard Overview":

    # ── Additional CSS for KPI section titles ─────────────────────────────
    st.markdown("""
    <style>
        .kpi-section-title {
            font-size: 1.2rem;
            font-weight: 800;
            color: #ffffff;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            margin: 30px 0 15px 0;
            padding-left: 15px;
            border-left: 4px solid #8b5cf6;
            text-shadow: 0 0 20px rgba(139, 92, 246, 0.3);
        }
        .kpi-divider { 
            border: none; 
            height: 1px;
            background: linear-gradient(90deg, rgba(139,92,246,0.3) 0%, transparent 100%); 
            margin: 24px 0; 
        }
    </style>
    """, unsafe_allow_html=True)

    # Header banner
    st.markdown("""
    <div class="header-banner">
        <h1>&#127978; DataFoundation</h1>
        <p>Multi-source Retail Data Integration Hub &#8212; Real-time monitoring &amp; analytics</p>
    </div>
    """, unsafe_allow_html=True)

    if not df_sales_raw.empty:
        df = df_sales_raw.copy()
        df['Date'] = pd.to_datetime(df['Date'])
        df['Month'] = df['Date'].dt.to_period('M').astype(str)

        # ── Core aggregations ─────────────────────────────────────────────
        total_revenue      = df['Total Amount'].sum()
        total_transactions = df['Transaction ID'].nunique()
        unique_customers   = df['Customer ID'].nunique()
        avg_order_value    = df['Total Amount'].mean()
        total_units_sold   = df['Quantity'].sum()
        num_categories     = df['Product Category'].nunique()
        avg_price_per_unit = df['Price per Unit'].mean()
        avg_qty_per_order  = df['Quantity'].mean()
        max_single_txn     = df['Total Amount'].max()
        api_products_count = len(df_api_products) if not df_api_products.empty else 0

        # Best month
        monthly_rev  = df.groupby('Month')['Total Amount'].sum()
        best_month   = monthly_rev.idxmax()
        best_month_rev = monthly_rev.max()

        # Top category
        cat_rev      = df.groupby('Product Category')['Total Amount'].sum()
        top_category = cat_rev.idxmax()
        top_cat_pct  = cat_rev.max() / total_revenue * 100

        # Gender revenue
        gender_rev   = df.groupby('Gender')['Total Amount'].sum()
        top_gender   = gender_rev.idxmax()
        top_gender_pct = gender_rev.max() / total_revenue * 100
        female_rev   = gender_rev.get('Female', 0)
        male_rev     = gender_rev.get('Male', 0)
        female_pct   = female_rev / total_revenue * 100

        # Rev per customer
        rev_per_customer = total_revenue / unique_customers

        # Age stats
        avg_cust_age  = df['Age'].mean()
        youngest_cust = df['Age'].min()
        oldest_cust   = df['Age'].max()

        # MoM growth
        monthly_sorted = monthly_rev.sort_index()
        if len(monthly_sorted) >= 2:
            prev_rev   = monthly_sorted.iloc[-2]
            last_rev   = monthly_sorted.iloc[-1]
            mom_growth = (last_rev - prev_rev) / prev_rev * 100 if prev_rev else 0
            mom_label  = f"{'+' if mom_growth >= 0 else ''}{mom_growth:.1f}%"
        else:
            mom_label = "N/A"

        # ── ROW 1: Revenue & Transaction KPIs ────────────────────────────
        st.markdown('<p class="kpi-section-title">&#128181; Revenue &amp; Transaction</p>',
                    unsafe_allow_html=True)
        r1c1, r1c2, r1c3, r1c4, r1c5, r1c6 = st.columns(6)
        with r1c1:
            st.metric("Total Revenue",     f"${total_revenue:,.0f}",     "+12.3%")
        with r1c2:
            st.metric("Transactions",      f"{total_transactions:,}",    "+8.7%")
        with r1c3:
            st.metric("Avg Order Value",   f"${avg_order_value:,.0f}",   "+3.1%")
        with r1c4:
            st.metric("Total Units Sold",  f"{total_units_sold:,}",      "+6.4%")
        with r1c5:
            st.metric("Avg Qty / Order",   f"{avg_qty_per_order:.2f}",   "units")
        with r1c6:
            st.metric("Max Single Order",  f"${max_single_txn:,.0f}",    "peak")

        st.markdown('<hr class="kpi-divider">', unsafe_allow_html=True)

        # ── ROW 2: Customer & Product KPIs ───────────────────────────────
        st.markdown('<p class="kpi-section-title">&#128101; Customer &amp; Product</p>',
                    unsafe_allow_html=True)
        r2c1, r2c2, r2c3, r2c4, r2c5, r2c6 = st.columns(6)
        with r2c1:
            st.metric("Unique Customers",   f"{unique_customers:,}",       "+5.2%")
        with r2c2:
            st.metric("Rev / Customer",     f"${rev_per_customer:,.0f}",   "LTV proxy")
        with r2c3:
            st.metric("Top Category",       top_category,                   f"{top_cat_pct:.1f}% share")
        with r2c4:
            st.metric("Top Gender Seg.",    top_gender,                     f"{top_gender_pct:.1f}% rev")
        with r2c5:
            st.metric("Best Month",         best_month,                     f"${best_month_rev:,.0f}")
        with r2c6:
            st.metric("API Products",       f"{api_products_count}",        "catalog items")

        st.markdown('<hr class="kpi-divider">', unsafe_allow_html=True)

        # ── ROW 3: Pricing & Growth KPIs ─────────────────────────────────
        st.markdown('<p class="kpi-section-title">&#128200; Pricing &amp; Growth</p>',
                    unsafe_allow_html=True)
        r3c1, r3c2, r3c3, r3c4, r3c5, r3c6 = st.columns(6)
        with r3c1:
            st.metric("Avg Price / Unit",  f"${avg_price_per_unit:,.0f}",  "per sku")
        with r3c2:
            st.metric("MoM Growth",        mom_label,                       "last month")
        with r3c3:
            st.metric("Avg Customer Age",  f"{avg_cust_age:.0f} yrs",      f"{youngest_cust}-{oldest_cust} range")
        with r3c4:
            st.metric("Female Rev Share",  f"{female_pct:.1f}%",           f"${female_rev:,.0f}")
        with r3c5:
            st.metric("Male Rev Share",    f"{100-female_pct:.1f}%",       f"${male_rev:,.0f}")
        with r3c6:
            st.metric("Product Categories", f"{num_categories}",            "active")

        st.divider()



        # ── Charts row (existing) ─────────────────────────────────────────
        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("#### &#128200; Monthly Revenue Trend")
            monthly_agg = df.groupby('Month').agg(
                Revenue=('Total Amount', 'sum'),
                Transactions=('Transaction ID', 'nunique'),
            ).reset_index()
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=monthly_agg['Month'],
                y=monthly_agg['Revenue'],
                mode='lines+markers',
                name='Revenue',
                line=dict(color='#8b5cf6', width=3, shape='spline', smoothing=1.2),
                marker=dict(
                    size=10,
                    color='#8b5cf6',
                    line=dict(width=2, color='rgba(255,255,255,0.6)'),
                    symbol='circle',
                ),
                fill='tozeroy',
                fillcolor='rgba(139, 92, 246, 0.12)',
            ))
            fig.update_layout(
                **CHART_TEMPLATE['layout'],
                height=350, showlegend=False, yaxis_title='Revenue ($)',
            )
            apply_chart_animation(fig)
            st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.markdown("#### &#127991; Revenue by Category")
            cat_rev_df = df.groupby('Product Category')['Total Amount'].sum().reset_index()
            fig = px.pie(
                cat_rev_df, values='Total Amount', names='Product Category',
                color_discrete_sequence=CHART_COLORS, hole=0.45,
            )
            fig.update_layout(
                **CHART_TEMPLATE['layout'], height=350, showlegend=True,
                legend=dict(orientation='h', yanchor='bottom', y=-0.15),
            )
            fig.update_traces(
                textinfo='percent+label',
                textfont_size=12,
                pull=[0.04] * len(cat_rev_df),
                hovertemplate='<b>%{label}</b><br>Revenue: $%{value:,.0f}<br>Share: %{percent}<extra></extra>',
            )
            apply_chart_animation(fig)
            st.plotly_chart(fig, use_container_width=True)

        # ── Additional KPI Charts ─────────────────────────────────────────
        st.divider()
        # st.markdown("#### &#128269; Additional KPI Visualisations")
        vc1, vc2= st.columns(2)

        with vc1:
            st.markdown("**Units Sold by Category**")
            units_cat = df.groupby('Product Category')['Quantity'].sum().reset_index()
            fig_u = px.bar(
                units_cat, x='Product Category', y='Quantity',
                color='Product Category', color_discrete_sequence=CHART_COLORS,
                text_auto=True,
            )
            fig_u.update_layout(**CHART_TEMPLATE['layout'], height=300,
                                showlegend=False, yaxis_title="Units Sold")
            fig_u.update_traces(
                textposition='inside',
                marker_line_width=0,
                hovertemplate='<b>%{x}</b><br>Units: %{y:,}<extra></extra>',
            )
            apply_chart_animation(fig_u)
            st.plotly_chart(fig_u, use_container_width=True)

        with vc2:
            st.markdown("**Avg Order Value by Gender & Category**")
            aov_gc = df.groupby(['Gender', 'Product Category'])['Total Amount'].mean().reset_index()
            fig_g = px.bar(
                aov_gc, x='Product Category', y='Total Amount',
                color='Gender', barmode='group',
                color_discrete_sequence=['#8b5cf6', '#ec4899'],
            )
            fig_g.update_layout(**CHART_TEMPLATE['layout'], height=300,
                                yaxis_title="Avg Order ($)")
            fig_g.update_traces(
                marker_line_width=0,
                hovertemplate='<b>%{x}</b> — %{fullData.name}<br>Avg Order: $%{y:,.0f}<extra></extra>',
            )
            apply_chart_animation(fig_g)
            st.plotly_chart(fig_g, use_container_width=True)

        # with vc3:
        #     st.markdown("**Revenue Growth Month-over-Month**")
        #     monthly_growth = df.groupby('Month')['Total Amount'].sum().reset_index()
        #     monthly_growth['MoM %'] = monthly_growth['Total Amount'].pct_change() * 100
        #     monthly_growth = monthly_growth.dropna()
        #     fig_m = px.bar(
        #         monthly_growth, x='Month', y='MoM %',
        #         color='MoM %', color_continuous_scale=['#ef4444', '#22c55e'],
        #         text_auto='.1f',
        #     )
        #     fig_m.update_layout(**CHART_TEMPLATE['layout'], height=300,
        #                         yaxis_title="MoM Growth (%)", showlegend=False)
        #     fig_m.update_traces(
        #         marker_line_width=0,
        #         hovertemplate='<b>%{x}</b><br>MoM Growth: %{y:.1f}%<extra></extra>',
        #     )
        #     apply_chart_animation(fig_m)
        #     st.plotly_chart(fig_m, use_container_width=True)

    st.markdown("""
    <div class="header-banner">
        <h1>📊 Sales Analytics</h1>
        <p>Sales performance data mart — trends, patterns, and insights</p>
    </div>
    """, unsafe_allow_html=True)
    
    if not df_sales_raw.empty:
        # Filters
        filter_col1, filter_col2 = st.columns(2)
        with filter_col1:
            selected_categories = st.multiselect(
                "Filter by Category",
                df_sales_raw['Product Category'].unique(),
                default=df_sales_raw['Product Category'].unique()
            )
        with filter_col2:
            selected_genders = st.multiselect(
                "Filter by Gender",
                df_sales_raw['Gender'].unique(),
                default=df_sales_raw['Gender'].unique()
            )
        
        df_filtered = df_sales_raw[
            (df_sales_raw['Product Category'].isin(selected_categories)) &
            (df_sales_raw['Gender'].isin(selected_genders))
        ]
        
        # KPIs
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        with kpi1:
            st.metric("Filtered Revenue", f"${df_filtered['Total Amount'].sum():,.0f}")
        with kpi2:
            st.metric("Transactions", f"{df_filtered['Transaction ID'].nunique():,}")
        with kpi3:
            st.metric("Avg Quantity", f"{df_filtered['Quantity'].mean():.1f}")
        with kpi4:
            st.metric("Avg Price/Unit", f"${df_filtered['Price per Unit'].mean():.0f}")
        
        st.divider()
        
        # Monthly trend
        st.markdown("#### 📈 Monthly Revenue & Transaction Trend")
        df_monthly = df_filtered.copy()
        df_monthly['Month'] = df_monthly['Date'].dt.to_period('M').astype(str)
        monthly = df_monthly.groupby('Month').agg(
            Revenue=('Total Amount', 'sum'),
            Transactions=('Transaction ID', 'nunique'),
            AvgOrder=('Total Amount', 'mean'),
        ).reset_index()
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(
                x=monthly['Month'], y=monthly['Revenue'],
                name='Revenue',
                marker=dict(
                    color='#8b5cf6',
                    opacity=0.8,
                    line=dict(width=0),
                ),
                hovertemplate='<b>%{x}</b><br>Revenue: $%{y:,.0f}<extra></extra>',
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=monthly['Month'], y=monthly['Transactions'],
                name='Transactions',
                line=dict(color='#06b6d4', width=3, shape='spline', smoothing=1.0),
                mode='lines+markers',
                marker=dict(size=9, line=dict(width=2, color='rgba(255,255,255,0.5)')),
                hovertemplate='<b>%{x}</b><br>Transactions: %{y:,}<extra></extra>',
            ),
            secondary_y=True,
        )
        fig.update_layout(
            **CHART_TEMPLATE['layout'],
            height=400,
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        )
        fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
        fig.update_yaxes(title_text="Transactions", secondary_y=True)
        apply_chart_animation(fig)
        st.plotly_chart(fig, use_container_width=True)
        
        # Category breakdown
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            st.markdown("#### 🏷️ Revenue by Category")
            cat_data = df_filtered.groupby('Product Category').agg(
                Revenue=('Total Amount', 'sum'),
                Qty=('Quantity', 'sum'),
            ).reset_index().sort_values('Revenue', ascending=True)
            
            fig = px.bar(
                cat_data, x='Revenue', y='Product Category',
                orientation='h',
                color='Product Category',
                color_discrete_sequence=CHART_COLORS,
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=300, showlegend=False)
            fig.update_traces(
                marker_line_width=0,
                hovertemplate='<b>%{y}</b><br>Revenue: $%{x:,.0f}<extra></extra>',
            )
            apply_chart_animation(fig)
            st.plotly_chart(fig, use_container_width=True)
        
        with chart_col2:
            st.markdown("#### 📊 Quantity Distribution")
            fig = px.histogram(
                df_filtered, x='Quantity', nbins=10,
                color='Product Category',
                color_discrete_sequence=CHART_COLORS,
                opacity=0.85,
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=300,
                              bargap=0.05)
            fig.update_traces(marker_line_width=0)
            apply_chart_animation(fig)
            st.plotly_chart(fig, use_container_width=True)
        
        # Daily heatmap
        st.markdown("#### 🗓️ Daily Sales Heatmap")
        df_heatmap = df_filtered.copy()
        df_heatmap['DayOfWeek'] = df_heatmap['Date'].dt.day_name()
        df_heatmap['Month'] = df_heatmap['Date'].dt.month_name()
        
        heatmap_data = df_heatmap.groupby(['DayOfWeek', 'Month'])['Total Amount'].sum().reset_index()
        heatmap_pivot = heatmap_data.pivot(
            index='DayOfWeek', columns='Month', values='Total Amount'
        ).fillna(0)
        
        # Order days
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        heatmap_pivot = heatmap_pivot.reindex(day_order)
        
        fig = px.imshow(
            heatmap_pivot.values,
            x=heatmap_pivot.columns.tolist(),
            y=heatmap_pivot.index.tolist(),
            color_continuous_scale=[
                [0.0, 'rgba(30,20,50,1)'],
                [0.3, 'rgba(100,40,180,1)'],
                [0.6, 'rgba(30,180,200,1)'],
                [1.0, 'rgba(255,220,100,1)'],
            ],
            aspect='auto',
            text_auto='$,.0f',
        )
        fig.update_layout(
            **CHART_TEMPLATE['layout'],
            height=320,
            coloraxis_colorbar=dict(
                title="Revenue ($)",
                tickformat='$,.0f',
                thickness=14,
                len=0.85,
            ),
        )
        fig.update_traces(
            hovertemplate='<b>%{y} — %{x}</b><br>Revenue: $%{z:,.0f}<extra></extra>',
        )
        apply_chart_animation(fig, duration=600)
        st.plotly_chart(fig, use_container_width=True)

        st.divider()
    st.markdown("""
    <div class="header-banner">
        <h1>🏷️ Category Analysis</h1>
        <p>Product category data mart — cross-source performance insights</p>
    </div>
    """, unsafe_allow_html=True)
    
    if not df_sales_raw.empty:
        # Category metrics
        cat_analysis = df_sales_raw.groupby('Product Category').agg(
            Revenue=('Total Amount', 'sum'),
            Transactions=('Transaction ID', 'nunique'),
            Quantity=('Quantity', 'sum'),
            AvgPrice=('Price per Unit', 'mean'),
            AvgOrderValue=('Total Amount', 'mean'),
            Customers=('Customer ID', 'nunique'),
            AvgAge=('Age', 'mean'),
        ).reset_index()
        
        total_rev = cat_analysis['Revenue'].sum()
        cat_analysis['RevenueShare'] = (
            cat_analysis['Revenue'] / total_rev * 100
        ).round(1)
        
        # KPIs per category
        for _, row in cat_analysis.iterrows():
            with st.expander(
                f"🏷️ **{row['Product Category']}** — "
                f"${row['Revenue']:,.0f} ({row['RevenueShare']}% share)",
                expanded=True
            ):
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Revenue", f"${row['Revenue']:,.0f}")
                m2.metric("Transactions", f"{row['Transactions']:,}")
                m3.metric("Units Sold", f"{row['Quantity']:,}")
                m4.metric("Avg Order", f"${row['AvgOrderValue']:,.0f}")
                m5.metric("Avg Age", f"{row['AvgAge']:.0f}")
        
        st.divider()
        
        # Comparative charts
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            st.markdown("#### Revenue vs Transaction Count")
            fig = px.scatter(
                cat_analysis,
                x='Transactions', y='Revenue',
                size='Quantity', color='Product Category',
                color_discrete_sequence=CHART_COLORS,
                hover_data=['AvgOrderValue', 'Customers'],
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=400)
            fig.update_traces(
                marker=dict(
                    sizemode='area',
                    sizeref=2. * cat_analysis['Quantity'].max() / (40.**2),
                    sizemin=8,
                    line=dict(width=2, color='rgba(255,255,255,0.3)'),
                ),
                hovertemplate=(
                    '<b>%{customdata[2]}</b><br>'
                    'Transactions: %{x:,}<br>Revenue: $%{y:,.0f}<br>'
                    'Avg Order: $%{customdata[0]:,.0f}<br>Customers: %{customdata[1]:,}'
                    '<extra></extra>'
                ),
            )
            apply_chart_animation(fig)
            st.plotly_chart(fig, use_container_width=True)
        
        with chart_col2:
            st.markdown("#### Gender Split by Category")
            gender_cat = df_sales_raw.groupby(
                ['Product Category', 'Gender']
            )['Total Amount'].sum().reset_index()
            
            fig = px.bar(
                gender_cat,
                x='Product Category', y='Total Amount',
                color='Gender',
                barmode='group',
                color_discrete_sequence=['#8b5cf6', '#06b6d4'],
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=400)
            fig.update_traces(
                marker_line_width=0,
                hovertemplate='<b>%{x}</b> — %{fullData.name}<br>Revenue: $%{y:,.0f}<extra></extra>',
            )
            apply_chart_animation(fig)
            st.plotly_chart(fig, use_container_width=True)
        
        st.divider()

        # --- NEW SECTION: Monthly Sales Filter & Trend Chart ---
        st.markdown("#### Monthly Sales & Units Sold by Category")
        
        # Assumes your date column is named 'Date'. Adjust if it is 'Order Date', etc.
        date_col = 'Date' 
        
        if date_col in df_sales_raw.columns:
            # Create a Year-Month string for grouping (e.g., '2023-01')
            df_sales_raw['YearMonth'] = pd.to_datetime(df_sales_raw[date_col]).dt.to_period('M').astype(str)
            
            # Interactive Filter
            all_categories = df_sales_raw['Product Category'].unique()
            selected_categories = st.multiselect(
                "Filter by Product Category:",
                options=all_categories,
                default=all_categories
            )
            
            if selected_categories:
                # Filter and aggregate
                df_monthly = df_sales_raw[df_sales_raw['Product Category'].isin(selected_categories)]
                monthly_cat = df_monthly.groupby(['YearMonth', 'Product Category']).agg(
                    Revenue=('Total Amount', 'sum'),
                    UnitsSold=('Quantity', 'sum')
                ).reset_index()
                
                # Sort chronologically
                monthly_cat = monthly_cat.sort_values('YearMonth')
                
                # Plotly Line Chart
                fig_monthly = px.line(
                    monthly_cat,
                    x='YearMonth', 
                    y='Revenue',
                    color='Product Category',
                    markers=True,
                    hover_data={'UnitsSold': True, 'Revenue': ':$,.0f'},
                    labels={'YearMonth': 'Month', 'Revenue': 'Monthly Revenue'},
                    color_discrete_sequence=CHART_COLORS
                )
                fig_monthly.update_layout(**CHART_TEMPLATE['layout'], height=400)
                fig_monthly.update_traces(
                    line=dict(width=2.5, shape='spline', smoothing=1.1),
                    marker=dict(size=8, line=dict(width=2, color='rgba(255,255,255,0.4)')),
                    hovertemplate='<b>%{x}</b><br>Revenue: $%{y:,.0f}<br>Units Sold: %{customdata[0]:,}<extra></extra>',
                )
                apply_chart_animation(fig_monthly)
                st.plotly_chart(fig_monthly, use_container_width=True)
            else:
                st.info("Please select at least one category to view the monthly trend.")
        else:
            st.warning(f"Could not find a date column named '{date_col}' to generate monthly trends.")
         # ── KPI Summary Table ─────────────────────────────────────────────
        st.markdown("#### &#128203;Summary by Category")
        cat_summary = df.groupby('Product Category').agg(
            Revenue      = ('Total Amount',   'sum'),
            Transactions = ('Transaction ID', 'nunique'),
            Customers    = ('Customer ID',    'nunique'),
            Units_Sold   = ('Quantity',       'sum'),
            Avg_Order    = ('Total Amount',   'mean'),
            Avg_Price    = ('Price per Unit', 'mean'),
            Avg_Qty      = ('Quantity',       'mean'),
            Avg_Age      = ('Age',            'mean'),
        ).reset_index()
        cat_summary['Rev_Share_%'] = (cat_summary['Revenue'] / total_revenue * 100).round(1)
        # Format for display
        display_summary = cat_summary.copy()
        display_summary['Revenue']    = display_summary['Revenue'].apply(lambda x: f"${x:,.0f}")
        display_summary['Avg_Order']  = display_summary['Avg_Order'].apply(lambda x: f"${x:,.0f}")
        display_summary['Avg_Price']  = display_summary['Avg_Price'].apply(lambda x: f"${x:,.0f}")
        display_summary['Avg_Qty']    = display_summary['Avg_Qty'].apply(lambda x: f"{x:.2f}")
        display_summary['Avg_Age']    = display_summary['Avg_Age'].apply(lambda x: f"{x:.0f}")
        display_summary['Rev_Share_%'] = display_summary['Rev_Share_%'].apply(lambda x: f"{x}%")
        display_summary.columns = [
            'Category', 'Revenue', 'Transactions', 'Customers',
            'Units Sold', 'Avg Order', 'Avg Price', 'Avg Qty', 'Avg Age', 'Rev Share %'
        ]
        st.dataframe(display_summary, use_container_width=True, hide_index=True)    

    else:
        st.warning("No sales data available. Please ensure `retail_sales_dataset.csv` is present.")


# ═══════════════════════════════════════════════════════════════════════
# PAGE: DATA SOURCES
# ═══════════════════════════════════════════════════════════════════════

elif page == "📂 Data Sources":
    st.markdown("""
    <div class="header-banner">
        <h1>📂 Data Sources</h1>
        <p>Explore raw data from Kaggle Retail Sales dataset and Fake Store API</p>
    </div>
    """, unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["📊 Retail Sales (CSV)", "🌐 Product Catalog (API)"])
    
    with tab1:
        st.markdown("### Kaggle Retail Sales Dataset")
        if not df_sales_raw.empty:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Records", f"{len(df_sales_raw):,}")
            with col2:
                st.metric("Date Range", f"{df_sales_raw['Date'].min().strftime('%Y-%m-%d')}")
            with col3:
                st.metric("Categories", f"{df_sales_raw['Product Category'].nunique()}")
            with col4:
                st.metric("Customers", f"{df_sales_raw['Customer ID'].nunique()}")
            
            st.dataframe(df_sales_raw.head(50), use_container_width=True, height=400)
            
            # Data quality
            st.markdown("#### 🔍 Data Quality Report")
            quality_col1, quality_col2 = st.columns(2)
            with quality_col1:
                null_counts = df_sales_raw.isnull().sum()
                st.markdown("**Null Values:**")
                for col_name, count in null_counts.items():
                    status = "✅" if count == 0 else "⚠️"
                    st.text(f"  {status} {col_name}: {count}")
            with quality_col2:
                st.markdown("**Column Types:**")
                for col_name, dtype in df_sales_raw.dtypes.items():
                    st.text(f"  📋 {col_name}: {dtype}")
    
    with tab2:
        st.markdown("### Fake Store API Products")
        if not df_api_products.empty:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Products", f"{len(df_api_products)}")
            with col2:
                st.metric("Categories", f"{df_api_products['category'].nunique()}")
            with col3:
                st.metric("Avg Price", f"${df_api_products['price'].mean():.2f}")
            with col4:
                st.metric("Avg Rating", f"{df_api_products['rating_rate'].mean():.1f} ⭐")
            
            st.dataframe(
                df_api_products[['id', 'title', 'price', 'category', 'rating_rate', 'rating_count']],
                use_container_width=True,
                height=400,
            )
            
            # Price distribution
            fig = px.histogram(
                df_api_products, x='price', nbins=20,
                color='category',
                color_discrete_sequence=CHART_COLORS,
                title="Product Price Distribution by Category"
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=350)
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# PAGE: ETL PIPELINE
# ═══════════════════════════════════════════════════════════════════════

elif page == "🔄 ETL Pipeline":
    st.markdown("""
    <div class="header-banner">
        <h1>🔄 ETL Pipeline</h1>
        <p>Execute and monitor the Extract → Transform → Load pipeline</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Pipeline architecture diagram
    st.markdown("### 🏗️ Pipeline Architecture")
    
    arch_col1, arch_col2, arch_col3, arch_col4, arch_col5 = st.columns(5)
    
    with arch_col1:
        st.markdown("""
        <div class="info-card" style="text-align: center;">
            <h3>📂</h3>
            <h4 style="color: #8b5cf6;">Sources</h4>
            <p style="font-size: 0.8rem; color: #888;">
                CSV Dataset<br>
                REST API
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with arch_col2:
        st.markdown("""
        <div class="info-card" style="text-align: center;">
            <h3>⬇️</h3>
            <h4 style="color: #06b6d4;">Extract</h4>
            <p style="font-size: 0.8rem; color: #888;">
                CSV Reader<br>
                API Client
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with arch_col3:
        st.markdown("""
        <div class="info-card" style="text-align: center;">
            <h3>🔄</h3>
            <h4 style="color: #f59e0b;">Transform</h4>
            <p style="font-size: 0.8rem; color: #888;">
                Clean & Validate<br>
                Star Schema Model
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with arch_col4:
        st.markdown("""
        <div class="info-card" style="text-align: center;">
            <h3>📤</h3>
            <h4 style="color: #22c55e;">Load</h4>
            <p style="font-size: 0.8rem; color: #888;">
                BigQuery<br>
                SCD Type 2
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with arch_col5:
        st.markdown("""
        <div class="info-card" style="text-align: center;">
            <h3>📊</h3>
            <h4 style="color: #ec4899;">Marts</h4>
            <p style="font-size: 0.8rem; color: #888;">
                Sales Perf<br>
                Category Analysis
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # Pipeline execution controls
    st.markdown("### 🎮 Pipeline Controls")
    
    run_col1, run_col2 = st.columns(2)
    
    with run_col1:
        skip_load = st.checkbox("Skip BigQuery Load (Transform Only)", value=True)
        
    with run_col2:
        if st.button("▶️ Run ETL Pipeline", type="primary", use_container_width=True):
            with st.spinner("🔄 Running ETL pipeline..."):
                try:
                    results = run_etl_pipeline(skip_load=skip_load)
                    
                    if results['status'] == 'success':
                        st.success(
                            f"✅ Pipeline completed in "
                            f"{results['total_duration_seconds']}s!"
                        )
                        st.json(results['stages'])
                    else:
                        st.error(f"❌ Pipeline failed: {results.get('error', 'Unknown error')}")
                except Exception as e:
                    st.error(f"❌ Error: {e}")
    
    # Table schema overview
    st.divider()
    st.markdown("### 🗂️ Data Warehouse Tables")
    
    tables_data = pd.DataFrame({
        'Table': [
            'stg_retail_sales', 'stg_api_products',
            'dim_date', 'dim_customer', 'dim_product', 'dim_product_category',
            'fact_sales',
            'mart_sales_performance', 'mart_category_analysis'
        ],
        'Layer': [
            'Staging', 'Staging',
            'Dimension', 'Dimension (SCD2)', 'Dimension (SCD2)', 'Dimension',
            'Fact',
            'Data Mart', 'Data Mart'
        ],
        'Source': [
            'CSV', 'API',
            'Generated', 'CSV', 'API + CSV', 'Both',
            'CSV',
            'Aggregated', 'Aggregated'
        ],
        'Load Strategy': [
            'Full Refresh', 'Full Refresh',
            'Full Refresh', 'SCD Type 2 Merge', 'SCD Type 2 Merge', 'Full Refresh',
            'Full Refresh',
            'Full Refresh', 'Full Refresh'
        ],
    })
    
    st.dataframe(tables_data, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════
# PAGE: SALES ANALYTICS
# ═══════════════════════════════════════════════════════════════════════

elif page == "📊 Sales Analytics":
    st.markdown("""
    <div class="header-banner">
        <h1>📊 Sales Analytics</h1>
        <p>Sales performance data mart — trends, patterns, and insights</p>
    </div>
    """, unsafe_allow_html=True)
    
    if not df_sales_raw.empty:
        # Filters
        filter_col1, filter_col2 = st.columns(2)
        with filter_col1:
            selected_categories = st.multiselect(
                "Filter by Category",
                df_sales_raw['Product Category'].unique(),
                default=df_sales_raw['Product Category'].unique()
            )
        with filter_col2:
            selected_genders = st.multiselect(
                "Filter by Gender",
                df_sales_raw['Gender'].unique(),
                default=df_sales_raw['Gender'].unique()
            )
        
        df_filtered = df_sales_raw[
            (df_sales_raw['Product Category'].isin(selected_categories)) &
            (df_sales_raw['Gender'].isin(selected_genders))
        ]
        
        # KPIs
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        with kpi1:
            st.metric("Filtered Revenue", f"${df_filtered['Total Amount'].sum():,.0f}")
        with kpi2:
            st.metric("Transactions", f"{df_filtered['Transaction ID'].nunique():,}")
        with kpi3:
            st.metric("Avg Quantity", f"{df_filtered['Quantity'].mean():.1f}")
        with kpi4:
            st.metric("Avg Price/Unit", f"${df_filtered['Price per Unit'].mean():.0f}")
        
        st.divider()
        
        # Monthly trend
        st.markdown("#### 📈 Monthly Revenue & Transaction Trend")
        df_monthly = df_filtered.copy()
        df_monthly['Month'] = df_monthly['Date'].dt.to_period('M').astype(str)
        monthly = df_monthly.groupby('Month').agg(
            Revenue=('Total Amount', 'sum'),
            Transactions=('Transaction ID', 'nunique'),
            AvgOrder=('Total Amount', 'mean'),
        ).reset_index()
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(
                x=monthly['Month'], y=monthly['Revenue'],
                name='Revenue',
                marker=dict(color='#8b5cf6', opacity=0.8, line=dict(width=0)),
                hovertemplate='<b>%{x}</b><br>Revenue: $%{y:,.0f}<extra></extra>',
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=monthly['Month'], y=monthly['Transactions'],
                name='Transactions',
                line=dict(color='#06b6d4', width=3, shape='spline', smoothing=1.0),
                mode='lines+markers',
                marker=dict(size=9, line=dict(width=2, color='rgba(255,255,255,0.5)')),
                hovertemplate='<b>%{x}</b><br>Transactions: %{y:,}<extra></extra>',
            ),
            secondary_y=True,
        )
        fig.update_layout(
            **CHART_TEMPLATE['layout'],
            height=400,
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        )
        fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
        fig.update_yaxes(title_text="Transactions", secondary_y=True)
        apply_chart_animation(fig)
        st.plotly_chart(fig, use_container_width=True)
        
        # Category breakdown
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            st.markdown("#### 🏷️ Revenue by Category")
            cat_data = df_filtered.groupby('Product Category').agg(
                Revenue=('Total Amount', 'sum'),
                Qty=('Quantity', 'sum'),
            ).reset_index().sort_values('Revenue', ascending=True)
            
            fig = px.bar(
                cat_data, x='Revenue', y='Product Category',
                orientation='h',
                color='Product Category',
                color_discrete_sequence=CHART_COLORS,
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=300, showlegend=False)
            fig.update_traces(
                marker_line_width=0,
                hovertemplate='<b>%{y}</b><br>Revenue: $%{x:,.0f}<extra></extra>',
            )
            apply_chart_animation(fig)
            st.plotly_chart(fig, use_container_width=True)
        
        with chart_col2:
            st.markdown("#### 📊 Quantity Distribution")
            fig = px.histogram(
                df_filtered, x='Quantity', nbins=10,
                color='Product Category',
                color_discrete_sequence=CHART_COLORS,
                opacity=0.85,
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=300, bargap=0.05)
            fig.update_traces(marker_line_width=0)
            apply_chart_animation(fig)
            st.plotly_chart(fig, use_container_width=True)
        
        # Daily heatmap
        st.markdown("#### 🗓️ Daily Sales Heatmap")
        df_heatmap = df_filtered.copy()
        df_heatmap['DayOfWeek'] = df_heatmap['Date'].dt.day_name()
        df_heatmap['Month'] = df_heatmap['Date'].dt.month_name()
        
        heatmap_data = df_heatmap.groupby(['DayOfWeek', 'Month'])['Total Amount'].sum().reset_index()
        heatmap_pivot = heatmap_data.pivot(
            index='DayOfWeek', columns='Month', values='Total Amount'
        ).fillna(0)
        
        # Order days
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        heatmap_pivot = heatmap_pivot.reindex(day_order)
        
        fig = px.imshow(
            heatmap_pivot.values,
            x=heatmap_pivot.columns.tolist(),
            y=heatmap_pivot.index.tolist(),
            color_continuous_scale=[
                [0.0, 'rgba(30,20,50,1)'],
                [0.3, 'rgba(100,40,180,1)'],
                [0.6, 'rgba(30,180,200,1)'],
                [1.0, 'rgba(255,220,100,1)'],
            ],
            aspect='auto',
            text_auto='$,.0f',
        )
        fig.update_layout(
            **CHART_TEMPLATE['layout'],
            height=320,
            coloraxis_colorbar=dict(
                title="Revenue ($)",
                tickformat='$,.0f',
                thickness=14,
                len=0.85,
            ),
        )
        fig.update_traces(
            hovertemplate='<b>%{y} — %{x}</b><br>Revenue: $%{z:,.0f}<extra></extra>',
        )
        apply_chart_animation(fig, duration=600)
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# 
elif page == "🏷️ Category Analysis":
    st.markdown("""
    <div class="header-banner">
        <h1>🏷️ Category Analysis</h1>
        <p>Product category data mart — cross-source performance insights</p>
    </div>
    """, unsafe_allow_html=True)
    
    if not df_sales_raw.empty:
        # Category metrics
        cat_analysis = df_sales_raw.groupby('Product Category').agg(
            Revenue=('Total Amount', 'sum'),
            Transactions=('Transaction ID', 'nunique'),
            Quantity=('Quantity', 'sum'),
            AvgPrice=('Price per Unit', 'mean'),
            AvgOrderValue=('Total Amount', 'mean'),
            Customers=('Customer ID', 'nunique'),
            AvgAge=('Age', 'mean'),
        ).reset_index()
        
        total_rev = cat_analysis['Revenue'].sum()
        cat_analysis['RevenueShare'] = (
            cat_analysis['Revenue'] / total_rev * 100
        ).round(1)
        
        # KPIs per category
        for _, row in cat_analysis.iterrows():
            with st.expander(
                f"🏷️ **{row['Product Category']}** — "
                f"${row['Revenue']:,.0f} ({row['RevenueShare']}% share)",
                expanded=True
            ):
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Revenue", f"${row['Revenue']:,.0f}")
                m2.metric("Transactions", f"{row['Transactions']:,}")
                m3.metric("Units Sold", f"{row['Quantity']:,}")
                m4.metric("Avg Order", f"${row['AvgOrderValue']:,.0f}")
                m5.metric("Avg Age", f"{row['AvgAge']:.0f}")
        
        st.divider()
        
        # Comparative charts
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            st.markdown("#### Revenue vs Transaction Count")
            fig = px.scatter(
                cat_analysis,
                x='Transactions', y='Revenue',
                size='Quantity', color='Product Category',
                color_discrete_sequence=CHART_COLORS,
                hover_data=['AvgOrderValue', 'Customers'],
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=400)
            fig.update_traces(
                marker=dict(
                    sizemode='area',
                    sizeref=2. * cat_analysis['Quantity'].max() / (40.**2),
                    sizemin=8,
                    line=dict(width=2, color='rgba(255,255,255,0.3)'),
                ),
                hovertemplate=(
                    '<b>%{customdata[2]}</b><br>'
                    'Transactions: %{x:,}<br>Revenue: $%{y:,.0f}<br>'
                    'Avg Order: $%{customdata[0]:,.0f}<br>Customers: %{customdata[1]:,}'
                    '<extra></extra>'
                ),
            )
            apply_chart_animation(fig)
            st.plotly_chart(fig, use_container_width=True)
        
        with chart_col2:
            st.markdown("#### Gender Split by Category")
            gender_cat = df_sales_raw.groupby(
                ['Product Category', 'Gender']
            )['Total Amount'].sum().reset_index()
            
            fig = px.bar(
                gender_cat,
                x='Product Category', y='Total Amount',
                color='Gender',
                barmode='group',
                color_discrete_sequence=['#8b5cf6', '#06b6d4'],
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=400)
            fig.update_traces(
                marker_line_width=0,
                hovertemplate='<b>%{x}</b> — %{fullData.name}<br>Revenue: $%{y:,.0f}<extra></extra>',
            )
            apply_chart_animation(fig)
            st.plotly_chart(fig, use_container_width=True)
        
        st.divider()

        # --- NEW SECTION: Monthly Sales Filter & Trend Chart ---
        st.markdown("#### Monthly Sales & Units Sold by Category")
        
        # Assumes your date column is named 'Date'. Adjust if it is 'Order Date', etc.
        date_col = 'Date' 
        
        if date_col in df_sales_raw.columns:
            # Create a Year-Month string for grouping (e.g., '2023-01')
            df_sales_raw['YearMonth'] = pd.to_datetime(df_sales_raw[date_col]).dt.to_period('M').astype(str)
            
            # Interactive Filter
            all_categories = df_sales_raw['Product Category'].unique()
            selected_categories = st.multiselect(
                "Filter by Product Category:",
                options=all_categories,
                default=all_categories
            )
            
            if selected_categories:
                # Filter and aggregate
                df_monthly = df_sales_raw[df_sales_raw['Product Category'].isin(selected_categories)]
                monthly_cat = df_monthly.groupby(['YearMonth', 'Product Category']).agg(
                    Revenue=('Total Amount', 'sum'),
                    UnitsSold=('Quantity', 'sum')
                ).reset_index()
                
                # Sort chronologically
                monthly_cat = monthly_cat.sort_values('YearMonth')
                
                # Plotly Line Chart
                fig_monthly = px.line(
                    monthly_cat,
                    x='YearMonth', 
                    y='Revenue',
                    color='Product Category',
                    markers=True,
                    hover_data={'UnitsSold': True, 'Revenue': ':$,.0f'},
                    labels={'YearMonth': 'Month', 'Revenue': 'Monthly Revenue'},
                    color_discrete_sequence=CHART_COLORS
                )
                fig_monthly.update_layout(**CHART_TEMPLATE['layout'], height=400)
                fig_monthly.update_traces(
                    line=dict(width=2.5, shape='spline', smoothing=1.1),
                    marker=dict(size=8, line=dict(width=2, color='rgba(255,255,255,0.4)')),
                    hovertemplate='<b>%{x}</b><br>Revenue: $%{y:,.0f}<br>Units Sold: %{customdata[0]:,}<extra></extra>',
                )
                apply_chart_animation(fig_monthly)
                st.plotly_chart(fig_monthly, use_container_width=True)
            else:
                st.info("Please select at least one category to view the monthly trend.")
        else:
            st.warning(f"Could not find a date column named '{date_col}' to generate monthly trends.")
            
        # --------------------------------------------------------
        
        # API product enrichment
        if not df_api_products.empty:
            st.divider()
            st.markdown("#### 🌐 API Product Catalog Enrichment")
            
            api_cat = df_api_products.groupby('category').agg(
                ProductCount=('id', 'count'),
                AvgPrice=('price', 'mean'),
                AvgRating=('rating_rate', 'mean'),
                TotalReviews=('rating_count', 'sum'),
            ).reset_index()
            
            fig = make_subplots(
                rows=1, cols=2,
                subplot_titles=("Avg Price by Category", "Avg Rating by Category"),
            )
            fig.add_trace(
                go.Bar(
                    x=api_cat['category'], y=api_cat['AvgPrice'],
                    marker=dict(color='#8b5cf6', line=dict(width=0)),
                    name='Avg Price',
                    hovertemplate='<b>%{x}</b><br>Avg Price: $%{y:.2f}<extra></extra>',
                ),
                row=1, col=1
            )
            fig.add_trace(
                go.Bar(
                    x=api_cat['category'], y=api_cat['AvgRating'],
                    marker=dict(color='#06b6d4', line=dict(width=0)),
                    name='Avg Rating',
                    hovertemplate='<b>%{x}</b><br>Avg Rating: %{y:.2f} ⭐<extra></extra>',
                ),
                row=1, col=2
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=350, showlegend=False)
            apply_chart_animation(fig)
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# PAGE: CUSTOMER INSIGHTS
# ═══════════════════════════════════════════════════════════════════════

elif page == "👤 Customer Insights":
    st.markdown("""
    <div class="header-banner">
        <h1>👤 Customer Insights</h1>
        <p>Customer dimension analysis — demographics, segments, and behavior</p>
    </div>
    """, unsafe_allow_html=True)
    
    if not df_sales_raw.empty:
        # Customer profile
        customers = df_sales_raw.groupby('Customer ID').agg(
            Gender=('Gender', 'first'),
            Age=('Age', 'first'),
            TotalSpent=('Total Amount', 'sum'),
            Transactions=('Transaction ID', 'nunique'),
            AvgOrder=('Total Amount', 'mean'),
            FirstPurchase=('Date', 'min'),
            LastPurchase=('Date', 'max'),
        ).reset_index()
        
        # Age groups
        customers['AgeGroup'] = pd.cut(
            customers['Age'],
            bins=[0, 25, 35, 45, 55, 65, 100],
            labels=['18-25', '26-35', '36-45', '46-55', '56-65', '65+']
        )
        
        # Top metrics
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("Total Customers", f"{len(customers):,}")
        kpi2.metric("Avg Lifetime Value", f"${customers['TotalSpent'].mean():,.0f}")
        kpi3.metric("Avg Age", f"{customers['Age'].mean():.0f}")
        kpi4.metric("Gender Split", f"♂ {(customers['Gender']=='Male').sum()} / ♀ {(customers['Gender']=='Female').sum()}")
        
        st.divider()
        
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            st.markdown("#### 📊 Age Distribution")
            fig = px.histogram(
                customers, x='Age', nbins=20,
                color='Gender',
                color_discrete_sequence=['#8b5cf6', '#ec4899'],
                barmode='overlay', opacity=0.75,
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=350, bargap=0.04)
            fig.update_traces(
                marker_line_width=0,
                hovertemplate='Age: %{x}<br>Count: %{y}<extra>%{fullData.name}</extra>',
            )
            apply_chart_animation(fig)
            st.plotly_chart(fig, use_container_width=True)
        
        with chart_col2:
            st.markdown("#### 💰 Revenue by Age Group")
            age_rev = customers.groupby('AgeGroup')['TotalSpent'].sum().reset_index()
            fig = px.bar(
                age_rev, x='AgeGroup', y='TotalSpent',
                color='AgeGroup',
                color_discrete_sequence=CHART_COLORS,
                text_auto='$,.0f',
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=350, showlegend=False)
            fig.update_traces(
                marker_line_width=0,
                textposition='outside',
                hovertemplate='<b>Age Group: %{x}</b><br>Total Spent: $%{y:,.0f}<extra></extra>',
            )
            apply_chart_animation(fig)
            st.plotly_chart(fig, use_container_width=True)
        
        # Spending distribution
        st.markdown("#### 📈 Customer Spending Distribution")
        fig = px.box(
            customers, x='Gender', y='TotalSpent',
            color='Gender',
            color_discrete_sequence=['#8b5cf6', '#ec4899'],
            notched=True,
            points='outliers',
        )
        fig.update_layout(**CHART_TEMPLATE['layout'], height=320)
        fig.update_traces(
            marker=dict(size=5, opacity=0.6, outliercolor='rgba(255,80,80,0.7)'),
            line=dict(width=2),
            hovertemplate='<b>%{x}</b><br>Spent: $%{y:,.0f}<extra></extra>',
        )
        apply_chart_animation(fig)
        st.plotly_chart(fig, use_container_width=True)
        
        # Top customers table
        st.markdown("#### 🏆 Top 20 Customers by Revenue")
        top_customers = customers.nlargest(20, 'TotalSpent')
        st.dataframe(
            top_customers[['Customer ID', 'Gender', 'Age', 'AgeGroup', 'TotalSpent', 'Transactions', 'AvgOrder']],
            use_container_width=True,
            hide_index=True,
        )


# ═══════════════════════════════════════════════════════════════════════
# PAGE: PRODUCT CATALOG
# ═══════════════════════════════════════════════════════════════════════

elif page == "📦 Product Catalog":
    st.markdown("""
    <div class="header-banner">
        <h1>📦 Product Catalog</h1>
        <p>Fake Store API product catalog — enriched product dimension</p>
    </div>
    """, unsafe_allow_html=True)
    
    if not df_api_products.empty:
        # KPIs
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("Products", f"{len(df_api_products)}")
        kpi2.metric("Categories", f"{df_api_products['category'].nunique()}")
        kpi3.metric("Avg Price", f"${df_api_products['price'].mean():.2f}")
        kpi4.metric("Avg Rating", f"{df_api_products['rating_rate'].mean():.1f} ⭐")
        
        st.divider()
        
        # Product cards
        selected_cat = st.selectbox(
            "Filter by Category",
            ['All'] + df_api_products['category'].unique().tolist()
        )
        
        if selected_cat != 'All':
            display_products = df_api_products[
                df_api_products['category'] == selected_cat
            ]
        else:
            display_products = df_api_products
        
        # Display in a grid
        cols = st.columns(4)
        for idx, (_, product) in enumerate(display_products.iterrows()):
            with cols[idx % 4]:
                st.markdown(f"""
                <div class="info-card" style="min-height: 200px;">
                    <p style="font-size: 0.75rem; color: #8b5cf6; text-transform: uppercase;">
                        {product['category']}
                    </p>
                    <h4 style="font-size: 0.9rem; color: #e0e0ff; line-height: 1.3;">
                        {product['title'][:50]}{'...' if len(product['title']) > 50 else ''}
                    </h4>
                    <p style="font-size: 1.2rem; color: #22c55e; font-weight: 700;">
                        ${product['price']:.2f}
                    </p>
                    <p style="font-size: 0.8rem; color: #f59e0b;">
                        ⭐ {product['rating_rate']} ({product['rating_count']} reviews)
                    </p>
                </div>
                """, unsafe_allow_html=True)
        
        st.divider()
        
        # Price vs Rating scatter
        st.markdown("#### 💎 Price vs Rating Analysis")
        fig = px.scatter(
            df_api_products,
            x='price', y='rating_rate',
            size='rating_count', color='category',
            hover_data=['title'],
            color_discrete_sequence=CHART_COLORS,
        )
        fig.update_layout(**CHART_TEMPLATE['layout'], height=400,
                          xaxis_title='Price ($)', yaxis_title='Rating (out of 5)')
        fig.update_traces(
            marker=dict(
                sizemode='area',
                sizeref=2. * df_api_products['rating_count'].max() / (40.**2),
                sizemin=7,
                line=dict(width=2, color='rgba(255,255,255,0.25)'),
                opacity=0.85,
            ),
            hovertemplate=(
                '<b>%{customdata[0]}</b><br>'
                'Price: $%{x:.2f}<br>'
                'Rating: %{y:.1f} ⭐<br>'
                'Reviews: %{marker.size:,}'
                '<extra></extra>'
            ),
        )
        apply_chart_animation(fig)
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# PAGE: DATA WAREHOUSE
# ═══════════════════════════════════════════════════════════════════════

elif page == "🗄️ Data Warehouse":
    st.markdown("""
    <div class="header-banner">
        <h1>🗄️ Data Warehouse</h1>
        <p>BigQuery schema explorer — star schema dimensional model</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Star Schema Diagram
    st.markdown("### ⭐ Star Schema Model")
    st.markdown("""
    ```
    ┌─────────────────┐     ┌─────────────────────────┐     ┌──────────────────┐
    │   dim_date       │     │      fact_sales          │     │  dim_customer    │
    ├─────────────────┤     ├─────────────────────────┤     ├──────────────────┤
    │ date_key    (PK) │◄───│ date_key          (FK)  │───►│ customer_key (PK)│
    │ full_date        │     │ customer_key      (FK)  │     │ customer_id      │
    │ year             │     │ category_key      (FK)  │     │ gender           │
    │ quarter          │     │ sales_key         (PK)  │     │ age / age_group  │
    │ month            │     │ transaction_id          │     │ customer_segment │
    │ day_name         │     │ quantity                │     │ SCD2 columns     │
    │ is_weekend       │     │ price_per_unit          │     └──────────────────┘
    └─────────────────┘     │ total_amount            │
                            └────────────┬────────────┘
                                         │
                            ┌────────────▼────────────┐     ┌──────────────────┐
                            │  dim_product_category    │     │   dim_product    │
                            ├─────────────────────────┤     ├──────────────────┤
                            │ category_key       (PK) │     │ product_key (PK) │
                            │ category_name            │     │ api_product_id   │
                            │ category_source          │     │ product_name     │
                            │ category_group           │     │ api_price        │
                            └─────────────────────────┘     │ SCD2 columns     │
                                                            └──────────────────┘
    ```
    """)
    
    st.divider()
    
    # SCD Type 2 explanation
    st.markdown("### 🔄 SCD Type 2 Implementation")
    st.markdown("""
    **Slowly Changing Dimension Type 2** is implemented for `dim_customer` and `dim_product` tables.
    
    | Column | Purpose |
    |--------|---------|
    | `effective_start_date` | When this version of the record became active |
    | `effective_end_date` | When this version was superseded (9999-12-31 for current) |
    | `is_current` | Boolean flag for the current active version |
    | `version` | Version number for tracking changes |
    | `row_hash` | MD5 hash of key attributes for change detection |
    """)
    
    st.divider()
    
    # BigQuery table browser
    st.markdown("### 📋 Table Browser")
    
    if use_bq:
        table_to_view = st.selectbox(
            "Select Table",
            ['stg_retail_sales', 'stg_api_products', 'dim_date', 'dim_customer',
             'dim_product', 'dim_product_category', 'fact_sales',
             'mart_sales_performance', 'mart_category_analysis']
        )
        
        if st.button("🔍 View Table Data"):
            with st.spinner("Loading from BigQuery..."):
                df = load_bigquery_data(table_to_view)
                if not df.empty:
                    st.dataframe(df, use_container_width=True, height=400)
                    st.info(f"📊 {len(df)} rows × {len(df.columns)} columns")
    else:
        st.info(
            "💡 Switch to **BigQuery (Live)** data source in the sidebar "
            "to browse warehouse tables."
        )
        
        # Show schema information
        st.markdown("#### Schema Definitions")
        schema_tab1, schema_tab2, schema_tab3 = st.tabs(
            ["Staging", "Dimensions", "Facts & Marts"]
        )
        
        with schema_tab1:
            st.code("""
CREATE TABLE stg_retail_sales (
    transaction_id INT64, date TIMESTAMP, customer_id STRING,
    gender STRING, age INT64, product_category STRING,
    quantity INT64, price_per_unit FLOAT64, total_amount FLOAT64,
    row_hash STRING, _extracted_at TIMESTAMP, _source STRING
);

CREATE TABLE stg_api_products (
    api_product_id INT64, product_name STRING, api_price FLOAT64,
    description STRING, product_category STRING, product_image_url STRING,
    rating_rate FLOAT64, rating_count INT64,
    _extracted_at TIMESTAMP, _source STRING
);
            """, language="sql")
        
        with schema_tab2:
            st.code("""
CREATE TABLE dim_customer (  -- SCD Type 2
    customer_key INT64, customer_id STRING, gender STRING,
    age INT64, age_group STRING, customer_segment STRING,
    first_purchase_date TIMESTAMP, last_purchase_date TIMESTAMP,
    total_transactions INT64,
    effective_start_date TIMESTAMP, effective_end_date TIMESTAMP,
    is_current BOOL, version INT64, row_hash STRING,
    _loaded_at TIMESTAMP
);

CREATE TABLE dim_product (  -- SCD Type 2
    product_key INT64, api_product_id INT64, product_name STRING,
    api_price FLOAT64, description STRING, product_category STRING,
    product_image_url STRING, rating_rate FLOAT64, rating_count INT64,
    effective_start_date TIMESTAMP, effective_end_date TIMESTAMP,
    is_current BOOL, version INT64, row_hash STRING,
    _loaded_at TIMESTAMP
);

CREATE TABLE dim_date (
    date_key INT64, full_date DATE, year INT64, quarter INT64,
    month INT64, month_name STRING, week_of_year INT64,
    day_of_month INT64, day_of_week INT64, day_name STRING,
    is_weekend BOOL, fiscal_year INT64, fiscal_quarter INT64
);
            """, language="sql")
        
        with schema_tab3:
            st.code("""
CREATE TABLE fact_sales (
    sales_key INT64, transaction_id INT64,
    date_key INT64, customer_key INT64, category_key INT64,
    quantity INT64, price_per_unit FLOAT64, total_amount FLOAT64,
    customer_id STRING, product_category STRING,
    gender STRING, age INT64,
    _extracted_at TIMESTAMP, _source STRING, _loaded_at TIMESTAMP
);

CREATE TABLE mart_sales_performance (
    year INT64, month INT64, month_name STRING,
    total_revenue FLOAT64, total_transactions INT64,
    total_quantity INT64, avg_order_value FLOAT64,
    unique_customers INT64, revenue_prev_month FLOAT64,
    revenue_growth_pct FLOAT64, _mart_generated_at TIMESTAMP
);

CREATE TABLE mart_category_analysis (
    product_category STRING, total_revenue FLOAT64,
    total_transactions INT64, total_quantity INT64,
    avg_price FLOAT64, avg_order_value FLOAT64,
    unique_customers INT64, avg_customer_age FLOAT64,
    revenue_share_pct FLOAT64, female_revenue_pct FLOAT64,
    male_revenue_pct FLOAT64, category_name STRING,
    category_group STRING, _mart_generated_at TIMESTAMP
);
            """, language="sql")


# ═══════════════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════════════

st.divider()
st.markdown(
    "<p style='text-align: center; color: #555; font-size: 0.8rem;'>"
    "🏪 DataFoundation: Multi-source Retail Data Integration Hub | "
    f"Built with Python, Streamlit & BigQuery | {datetime.now().year}"
    "</p>",
    unsafe_allow_html=True
)
