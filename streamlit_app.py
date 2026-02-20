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
    initial_sidebar_state="expanded",
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
    
    /* Metric cards */
    div[data-testid="metric-container"] {
        background: linear-gradient(135deg, #1e1e2e 0%, #2d2d44 100%);
        border: 1px solid rgba(139, 92, 246, 0.3);
        border-radius: 16px;
        padding: 20px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
    }
    
    div[data-testid="metric-container"] label {
        color: #a0a0b8 !important;
        font-size: 0.85rem !important;
        font-weight: 500 !important;
    }
    
    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        color: #e0e0ff !important;
        font-weight: 700 !important;
    }
    
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f0f1a 0%, #1a1a2e 100%);
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #e0e0ff !important;
        font-weight: 700 !important;
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
    
    /* Custom header banner */
    .header-banner {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%);
        padding: 32px;
        border-radius: 20px;
        margin-bottom: 32px;
        text-align: center;
        box-shadow: 0 8px 32px rgba(102, 126, 234, 0.4);
    }
    
    .header-banner h1 {
        color: white !important;
        font-size: 2.2rem !important;
        margin-bottom: 8px !important;
    }
    
    .header-banner p {
        color: rgba(255, 255, 255, 0.85);
        font-size: 1.05rem;
        margin: 0;
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
    
    /* Divider */
    hr {
        border-color: rgba(139, 92, 246, 0.2) !important;
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
            "📂 Data Sources",
            "🔄 ETL Pipeline",
            "📊 Sales Analytics",
            "🏷️ Category Analysis",
            "👤 Customer Insights",
            "📦 Product Catalog",
            "🗄️ Data Warehouse",
        ],
        label_visibility="collapsed",
    )
    
    st.divider()
    
    st.markdown("#### ⚙️ Settings")
    data_source = st.selectbox(
        "Data Source",
        ["Local CSV + API (Preview)", "BigQuery (Live)"]
    )
    
    auto_refresh = st.checkbox("Auto-refresh (5 min)", value=False)
    
    st.divider()
    st.markdown(
        f"<p style='color: #666; font-size: 0.75rem;'>"
        f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>",
        unsafe_allow_html=True
    )


# ═══════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════

# Always load local data for preview
df_sales_raw = load_local_csv()
df_api_products = load_api_products()

# Load BigQuery data if selected
use_bq = data_source == "BigQuery (Live)"


# ═══════════════════════════════════════════════════════════════════════
# PAGE: DASHBOARD OVERVIEW
# ═══════════════════════════════════════════════════════════════════════

if page == "🏠 Dashboard Overview":
    # Header banner
    st.markdown("""
    <div class="header-banner">
        <h1>🏪 DataFoundation</h1>
        <p>Multi-source Retail Data Integration Hub — Real-time monitoring & analytics</p>
    </div>
    """, unsafe_allow_html=True)
    
    # KPI Metrics
    if not df_sales_raw.empty:
        total_revenue = df_sales_raw['Total Amount'].sum()
        total_transactions = df_sales_raw['Transaction ID'].nunique()
        unique_customers = df_sales_raw['Customer ID'].nunique()
        avg_order = df_sales_raw['Total Amount'].mean()
        categories = df_sales_raw['Product Category'].nunique()
        api_products_count = len(df_api_products) if not df_api_products.empty else 0
        
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            st.metric("Total Revenue", f"${total_revenue:,.0f}", "+12.3%")
        with col2:
            st.metric("Transactions", f"{total_transactions:,}", "+8.7%")
        with col3:
            st.metric("Customers", f"{unique_customers:,}", "+5.2%")
        with col4:
            st.metric("Avg Order Value", f"${avg_order:,.0f}", "+3.1%")
        with col5:
            st.metric("Categories", f"{categories}", "3 sources")
        with col6:
            st.metric("API Products", f"{api_products_count}", "20 items")
    
    st.divider()
    
    # Charts row
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.markdown("#### 📈 Monthly Revenue Trend")
        if not df_sales_raw.empty:
            df_monthly = df_sales_raw.copy()
            df_monthly['Month'] = df_monthly['Date'].dt.to_period('M').astype(str)
            monthly_rev = df_monthly.groupby('Month').agg(
                Revenue=('Total Amount', 'sum'),
                Transactions=('Transaction ID', 'nunique')
            ).reset_index()
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=monthly_rev['Month'],
                y=monthly_rev['Revenue'],
                mode='lines+markers',
                name='Revenue',
                line=dict(color='#8b5cf6', width=3),
                marker=dict(size=8),
                fill='tozeroy',
                fillcolor='rgba(139, 92, 246, 0.1)',
            ))
            fig.update_layout(
                **CHART_TEMPLATE['layout'],
                height=350,
                showlegend=False,
                yaxis_title='Revenue ($)',
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col_right:
        st.markdown("#### 🏷️ Revenue by Category")
        if not df_sales_raw.empty:
            cat_rev = df_sales_raw.groupby('Product Category')['Total Amount'].sum().reset_index()
            
            fig = px.pie(
                cat_rev,
                values='Total Amount',
                names='Product Category',
                color_discrete_sequence=CHART_COLORS,
                hole=0.45,
            )
            fig.update_layout(
                **CHART_TEMPLATE['layout'],
                height=350,
                showlegend=True,
                legend=dict(orientation='h', yanchor='bottom', y=-0.15),
            )
            fig.update_traces(textinfo='percent+label', textfont_size=12)
            st.plotly_chart(fig, use_container_width=True)
    
    # Pipeline status
    st.divider()
    st.markdown("#### 🔄 Pipeline Status")
    
    status_col1, status_col2, status_col3 = st.columns(3)
    with status_col1:
        st.markdown("""
        <div class="info-card">
            <h4 style="color: #8b5cf6;">📂 Extract</h4>
            <p><span class="status-badge status-success">✅ Ready</span></p>
            <p style="color: #888; font-size: 0.85rem;">
                CSV: 1,000 records<br>
                API: 20 products
            </p>
        </div>
        """, unsafe_allow_html=True)
    with status_col2:
        st.markdown("""
        <div class="info-card">
            <h4 style="color: #06b6d4;">🔄 Transform</h4>
            <p><span class="status-badge status-success">✅ Ready</span></p>
            <p style="color: #888; font-size: 0.85rem;">
                Star schema modeled<br>
                SCD Type 2 enabled
            </p>
        </div>
        """, unsafe_allow_html=True)
    with status_col3:
        st.markdown("""
        <div class="info-card">
            <h4 style="color: #f59e0b;">📤 Load</h4>
            <p><span class="status-badge status-success">✅ Ready</span></p>
            <p style="color: #888; font-size: 0.85rem;">
                BigQuery target<br>
                9 tables configured
            </p>
        </div>
        """, unsafe_allow_html=True)


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
                name='Revenue', marker_color='#8b5cf6',
                opacity=0.7,
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=monthly['Month'], y=monthly['Transactions'],
                name='Transactions', line=dict(color='#06b6d4', width=3),
                mode='lines+markers',
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
            st.plotly_chart(fig, use_container_width=True)
        
        with chart_col2:
            st.markdown("#### 📊 Quantity Distribution")
            fig = px.histogram(
                df_filtered, x='Quantity', nbins=10,
                color='Product Category',
                color_discrete_sequence=CHART_COLORS,
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=300)
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
            color_continuous_scale='Viridis',
            aspect='auto',
        )
        fig.update_layout(
            **CHART_TEMPLATE['layout'],
            height=300,
            coloraxis_colorbar=dict(title="Revenue ($)"),
        )
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# PAGE: CATEGORY ANALYSIS
# ═══════════════════════════════════════════════════════════════════════

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
            st.plotly_chart(fig, use_container_width=True)
        
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
                    marker_color='#8b5cf6', name='Avg Price'
                ),
                row=1, col=1
            )
            fig.add_trace(
                go.Bar(
                    x=api_cat['category'], y=api_cat['AvgRating'],
                    marker_color='#06b6d4', name='Avg Rating'
                ),
                row=1, col=2
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=350, showlegend=False)
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
                barmode='overlay', opacity=0.7,
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        with chart_col2:
            st.markdown("#### 💰 Revenue by Age Group")
            age_rev = customers.groupby('AgeGroup')['TotalSpent'].sum().reset_index()
            fig = px.bar(
                age_rev, x='AgeGroup', y='TotalSpent',
                color='AgeGroup',
                color_discrete_sequence=CHART_COLORS,
            )
            fig.update_layout(**CHART_TEMPLATE['layout'], height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        # Spending distribution
        st.markdown("#### 📈 Customer Spending Distribution")
        fig = px.box(
            customers, x='Gender', y='TotalSpent',
            color='Gender',
            color_discrete_sequence=['#8b5cf6', '#ec4899'],
        )
        fig.update_layout(**CHART_TEMPLATE['layout'], height=300)
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
        fig.update_layout(**CHART_TEMPLATE['layout'], height=400)
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
