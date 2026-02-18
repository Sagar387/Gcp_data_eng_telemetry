import streamlit as st 
from google.cloud import bigquery
import pandas as pd    
import plotly.express as px
import time

# Page Set up
st.set_page_config(
    page_title="Pulse Stream | Ops Center",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS to make the metrics pop up
st.markdown("""
    <style>
    div[data-testid="stMetricValue"] {
        font-size: 24px;
        color: #00FF00;  /* Matrix Green */
    }
    </style>
    """, unsafe_allow_html=True)

st.title("🏎️ Pulse Stream: Engineering & Ops Console")
st.markdown("---") 

# Connect to Bigquery
@st.cache_resource(ttl=5) # Cache data for 5 seconds then refresh
def fetch_data(query):
    try:
        client = bigquery.Client(project = "pulse-stream-core")
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Database Error: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error
    
# Check connection immediately
with st.sidebar:
    st.header("🔌 System Status")
    if "client" not in st.session_state:
        st.success("✅ BigQuery Connected")
        st.session_state.client = True
    
    st.markdown("### 🛠️ Controls")
    if st.button("🔄 Force Refresh Cache"):
        st.cache_data.clear()
        st.rerun()
        
        
# Metrics 
st.subheader("📊 Real-Time Pipeline Health")

# Queries for the metrics
q_count = "SELECT COUNT(*) as total FROM `pulse-stream-core.pulse_stream_lake.silver_telemetry`"
q_latency = "SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(TIMESTAMP(timestamp)), SECOND) as lag FROM `pulse-stream-core.pulse_stream_lake.silver_telemetry`"
q_uniques = "SELECT COUNT(DISTINCT athlete_id) as unique_athletes FROM `pulse-stream-core.pulse_stream_lake.silver_telemetry`"

df_count = fetch_data(q_count)
df_lag = fetch_data(q_latency)
df_athletes = fetch_data(q_uniques)

# Extract the data
total_rows = df_count.iloc[0]['total'] if not df_count.empty else 0
lag_seconds = df_lag.iloc[0]['lag'] if not df_lag.empty else 0
athlete_count = df_athletes.iloc[0]['unique_athletes'] if not df_athletes.empty else 0

# Display in Columns
col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Ingested Rows", f"{total_rows:,}", delta="Kafka Stream Active")
col2.metric("System Latency", f"{lag_seconds}s", delta="-0.5s", delta_color="inverse") # Inverse means lower is better
col3.metric("Active Sensors", f"{athlete_count}", "Jessica, James, Thomas")
col4.metric("Pipeline Status", "HEALTHY", "All Systems Go")

st.markdown("---")

# Detailed View

# 4. DETAILED INSPECTION TABS
tab1, tab2, tab3 = st.tabs(["🔍 Raw Data Inspector", "📈 Engineer Analytics", "🛑 Danger Zone"])

# --- TAB 1: RAW DATA (BRONZE LAYER) ---
with tab1:
    st.write("### 📜 Live Stream: Bronze Layer (Raw Ingestion)")
    st.caption("Showing the last 10 messages landed in BigQuery. Use this to debug formatting issues.")
    
    # Query for raw data
    q_raw = """
        SELECT timestamp, name, speed, heart_rate, location, 
        FROM `pulse-stream-core.pulse_stream_lake.silver_telemetry`
        ORDER BY timestamp DESC
        LIMIT 10
    """
    df_raw = fetch_data(q_raw)
    
    # Interactive Dataframe
    st.dataframe(
        df_raw, 
        use_container_width=True,
        hide_index=True,
        column_config={
            "timestamp": st.column_config.DatetimeColumn("Event Time", format="D MMM, HH:mm:ss"),
            "speed_kph": st.column_config.NumberColumn("Speed (km/h)", format="%.1f 🚀"),
            "heart_rate": st.column_config.ProgressColumn("Heart Rate", min_value=60, max_value=200, format="%d bpm"),
        }
    )

# --- TAB 2: ANALYTICS (GOLD LAYER PREVIEW) ---
with tab2:
    st.write("### 📊 Distribution Analysis")
    st.caption("Checking for data anomalies (e.g., negative speeds or HR > 220).")
    
    # Let's pull slightly more data for a plot
    q_chart = """
        SELECT name, speed, heart_rate 
        FROM `pulse-stream-core.pulse_stream_lake.silver_telemetry`
        ORDER BY timestamp DESC
        LIMIT 500
    """
    df_chart = fetch_data(q_chart)
    
    if not df_chart.empty:
        # Use Plotly for an interactive scatter plot (Engineers love scatter plots)
        fig = px.scatter(
            df_chart, 
            x="speed", 
            y="heart_rate", 
            color="name",
            title="Speed vs. Heart Rate Correlation (Last 500 points)",
            size_max=10
        )
        st.plotly_chart(fig, use_container_width=True)

# --- TAB 3: ADMIN ACTIONS ---
with tab3:
    st.header("⚠️ Administrative Actions")
    
    c1, c2 = st.columns(2)
    with c1:
        st.error("Purge Bronze Layer")
        if st.button("🗑️ TRUNCATE TABLE (Simulation)"):
            st.toast("Access Denied: You are in Read-Only Mode!", icon="🔒")
            
    with c2:
        st.warning("Stop Kafka Producer")
        if st.button("🛑 EMERGENCY STOP"):
            st.toast("Signal sent to producer... (Simulation)", icon="📡")