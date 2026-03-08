import datetime
import streamlit as st
import duckdb
import pandas as pd
import os
import altair as alt
import shutil

st.set_page_config(layout="wide")

# Paths
write_db_path = "../transformation/weather_reports.db"
read_db_path = "../visualization/weather_reports_ro.db"

# Wait for DB to exist (same UX as landing page)
while not (os.path.exists(write_db_path)):
    st.warning("Waiting for the transformed database to be created...")
    st.sleep(5)

# Attempt to copy latest write DB to read DB (non-fatal)
try:
    # copy if exists; this keeps the same approach used on landing_page
    shutil.copy(write_db_path, read_db_path)
except Exception:
    pass

# compute mtime for cache invalidation
if os.path.exists(read_db_path):
    db_mtime_ts = os.path.getmtime(read_db_path)
else:
    db_mtime_ts = None

if 'overall_refresh' not in st.session_state:
    st.session_state.overall_refresh = 0
# ensure db_mtime_ts key exists in session state
if 'db_mtime_ts' not in st.session_state:
    st.session_state.db_mtime_ts = db_mtime_ts

# Refresh button
_, refresh_col = st.columns([9, 1])
with refresh_col:
    if st.button("Refresh"):
        if os.path.exists(read_db_path):
            # update mtime to bust cache
            st.session_state.db_mtime_ts = os.path.getmtime(read_db_path)
        st.cache_data.clear()
        st.session_state.overall_refresh += 1
        # Streamlit automatically reruns after interaction; no explicit st.experimental_rerun() required

@st.cache_data
def load_overall_metrics(db_path: str, db_mtime: float) -> dict:
    conn = duckdb.connect(database=db_path, read_only=True)

    now = datetime.datetime.utcnow()
    threshold_30m = (now - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
    threshold_24h = (now - datetime.timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')

    # Total distinct stations
    total_stations_q = "SELECT COUNT(DISTINCT station_name) as total FROM fact_observations;"
    total_stations = conn.execute(total_stations_q).df()['total'].iloc[0]

    # Stations with at least one temperature reading
    stations_with_temp_q = "SELECT COUNT(DISTINCT station_name) as total FROM fact_observations WHERE temperature_degC IS NOT NULL;"
    stations_with_temp = conn.execute(stations_with_temp_q).df()['total'].iloc[0]

    # Active stations in last 30 minutes
    active_stations_q = f"SELECT COUNT(DISTINCT station_name) as total FROM fact_observations WHERE observation_timestamp >= TIMESTAMP '{threshold_30m}';"
    try:
        active_stations = conn.execute(active_stations_q).df()['total'].iloc[0]
    except Exception:
        # fallback: if observation_timestamp not comparable, try casting
        active_stations_q = f"SELECT COUNT(DISTINCT station_name) as total FROM fact_observations WHERE CAST(observation_timestamp AS TIMESTAMP) >= TIMESTAMP '{threshold_30m}';"
        active_stations = conn.execute(active_stations_q).df()['total'].iloc[0]

    # Top stations by readings in last 24h
    top_stations_q = f"""
        SELECT station_name, COUNT(*) AS cnt
        FROM fact_observations
        WHERE observation_timestamp >= TIMESTAMP '{threshold_24h}'
        GROUP BY station_name
        ORDER BY cnt DESC
        LIMIT 10;
    """
    try:
        top_stations = conn.execute(top_stations_q).df()
    except Exception:
        top_stations_q = f"""
            SELECT station_name, COUNT(*) AS cnt
            FROM fact_observations
            WHERE CAST(observation_timestamp AS TIMESTAMP) >= TIMESTAMP '{threshold_24h}'
            GROUP BY station_name
            ORDER BY cnt DESC
            LIMIT 10;
        """
        top_stations = conn.execute(top_stations_q).df()

    # Readings per hour last 24h
    per_hour_q = f"""
        SELECT DATE_TRUNC('hour', CAST(observation_timestamp AS TIMESTAMP)) AS hour, COUNT(*) AS cnt
        FROM fact_observations
        WHERE CAST(observation_timestamp AS TIMESTAMP) >= TIMESTAMP '{threshold_24h}'
        GROUP BY hour
        ORDER BY hour;
    """
    try:
        per_hour = conn.execute(per_hour_q).df()
    except Exception:
        # try without CAST if unnecessary
        per_hour_q = f"""
            SELECT DATE_TRUNC('hour', observation_timestamp) AS hour, COUNT(*) AS cnt
            FROM fact_observations
            WHERE observation_timestamp >= TIMESTAMP '{threshold_24h}'
            GROUP BY hour
            ORDER BY hour;
        """
        per_hour = conn.execute(per_hour_q).df()

    return {
        'total_stations': int(total_stations),
        'stations_with_temp': int(stations_with_temp),
        'active_stations': int(active_stations),
        'top_stations': top_stations,
        'per_hour': per_hour,
        'fetched_at': now.strftime('%Y-%m-%d %H:%M:%S')
    }

# Load metrics
metrics = load_overall_metrics(read_db_path, db_mtime_ts)

st.title("Overall Metrics")

# Metrics cards
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total stations", metrics['total_stations'])
col2.metric("Stations w/ temperature", metrics['stations_with_temp'])
col3.metric("Active (last 30m)", metrics['active_stations'])
col4.metric("Data fetched at", metrics['fetched_at'])

st.markdown("---")

# Top stations bar chart
st.subheader("Top stations by readings (last 24h)")
if not metrics['top_stations'].empty:
    chart = alt.Chart(metrics['top_stations']).mark_bar().encode(
        x=alt.X('cnt:Q', title='Readings'),
        y=alt.Y('station_name:N', sort='-x', title='Station')
    ).properties(height=300)
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("No readings in last 24 hours.")

st.markdown("---")

# Readings per hour chart
st.subheader("Readings per hour (last 24h)")
if not metrics['per_hour'].empty:
    metrics['per_hour']['hour'] = pd.to_datetime(metrics['per_hour']['hour'])
    line = alt.Chart(metrics['per_hour']).mark_line(point=True).encode(
        x=alt.X('hour:T', title='Hour', axis=alt.Axis(format='%Y-%m-%d\n%H:%M', labelAngle=-45)),
        y=alt.Y('cnt:Q', title='Readings')
    ).properties(height=300)
    st.altair_chart(line, use_container_width=True)
else:
    st.info("No readings in last 24 hours.")

# show raw table option
with st.expander('Show top stations table'):
    st.dataframe(metrics['top_stations'])


