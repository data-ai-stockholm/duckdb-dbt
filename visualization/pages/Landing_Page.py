import datetime
import streamlit as st
import duckdb
import pandas as pd
import shutil
import os
import altair as alt

# Page layout settings
st.set_page_config(
    layout="wide"
)

# Creating the read-only database for visualization
write_db_path = "../transformation/weather_reports.db"
read_db_path = "../visualization/weather_reports_ro.db"

while not (os.path.exists(write_db_path)):
    st.warning("Waiting for the transformed database to be created...")
    st.sleep(5)

try:
    # Copy the file to the destination directory
    # If the destination is a directory, the file will be copied into it
    # with its original filename.
    shutil.copy(write_db_path, read_db_path)
    print(f"File '{write_db_path}' successfully copied to '{read_db_path}'.")
except FileNotFoundError:
    print(f"Error: Source file '{write_db_path}' not found.")
except Exception as e:
    print(f"An error occurred: {e}")

# after copying the DB file
if os.path.exists(read_db_path):
    db_mtime_ts = os.path.getmtime(read_db_path)
else:
    db_mtime_ts = None

# persist mtime and a refresh counter in session state so the UI can trigger reloads
if 'refresh_counter' not in st.session_state:
    st.session_state.refresh_counter = 0
if 'db_mtime_ts' not in st.session_state:
    st.session_state.db_mtime_ts = db_mtime_ts

# LOAD DATA FOR FRONTEND USAGE
@st.cache_data
def load_station_data(db_path: str, db_mtime: float) -> pd.DataFrame:
    """Return distinct station names from the DB. db_mtime is a dummy param to bust cache when the file changes."""
    conn = duckdb.connect(database=db_path, read_only=True)
    query = """
        SELECT DISTINCT station_name
        FROM fact_observations
        ORDER BY station_name
    """
    df = conn.execute(query).df()
    return df

@st.cache_data
def load_weather_data(db_path: str, weather_date_range: tuple, station_name, db_mtime: float) -> pd.DataFrame:
    """Return observations for the given date range and optional station_name list."""
    conn = duckdb.connect(database=db_path, read_only=True)
    start_date = weather_date_range[0].strftime("%Y-%m-%d")
    end_date = weather_date_range[1].strftime("%Y-%m-%d")

    # Build station filter
    station_clause = ""
    if station_name:
        # multiselect returns a list; ensure SQL tuple formatting
        if isinstance(station_name, (list, tuple)):
            station_tuple = tuple(station_name)
        else:
            station_tuple = (station_name,)
        # If single element, ensure trailing comma in tuple string if necessary
        station_clause = f" AND station_name IN {station_tuple}"

    query = f"""
        SELECT *
        FROM fact_observations
        WHERE DATE(observation_timestamp) BETWEEN '{start_date}' AND '{end_date}'
        {station_clause}
    """

    df = conn.execute(query).df()
    return df

# STREAMLIT FRONTEND
st.header("Weather Reports Application")

# Refresh button placed to the right of the header; clicking it updates the stored mtime,
# clears Streamlit's cached data, increments a counter and reruns the app to show fresh data.
_, refresh_col = st.columns([9,1])
with refresh_col:
    if st.button("Refresh data"):
        if os.path.exists(read_db_path):
            st.session_state.db_mtime_ts = os.path.getmtime(read_db_path)
        else:
            st.session_state.db_mtime_ts = None
        # clear cached results so next calls read fresh data
        st.cache_data.clear()
        st.session_state.refresh_counter += 1

date_range_filter_column, station_filter_column = st.columns(2)

# Date input for weather data filtering
today = datetime.datetime.now()
next_year = today.year + 1
jan_01 = datetime.date(today.year, 1, 1)
dec_31 = datetime.date(today.year, 12, 31)

with date_range_filter_column:
    weather_date_range = st.date_input(
        "Filter the date range for weather data:",
        (jan_01, datetime.date(today.year, 12, 31)),
        jan_01,
        dec_31,
        format="MM.DD.YYYY",
    )

with station_filter_column:
    selected_station = st.multiselect(
        "Pick the station:",
        load_station_data(read_db_path, st.session_state.db_mtime_ts)['station_name'].tolist(),
        max_selections=1,
        accept_new_options=False,
        default=['University of Miami']  # Temporarily set a default station (as a list)
    )

chart1, chart2 = st.columns(2)
chart3, chart4 = st.columns(2)
chart5, chart6 = st.columns(2)
chart7, chart8 = st.columns(2)
chart9, chart10 = st.columns(2)

try:
    weather_data_df = load_weather_data(read_db_path, weather_date_range, selected_station, st.session_state.db_mtime_ts)
    if weather_data_df.empty:
        st.info("No records found in weather data table.")
    else:
        # ensure timestamp column is datetime
        weather_data_df['observation_timestamp'] = pd.to_datetime(weather_data_df['observation_timestamp'])

        # helper to build altair line charts with date on first line and time on second
        def make_chart(df, y_col, y_label):
            chart = alt.Chart(df).mark_line(point=False).encode(
                x=alt.X('observation_timestamp:T', title='Date / Time',
                        axis=alt.Axis(format='%Y-%m-%d\n%H:%M', labelAngle=-45)),
                y=alt.Y(f'{y_col}:Q', title=y_label),
                tooltip=[alt.Tooltip('observation_timestamp:T', title='Datetime'), alt.Tooltip(f'{y_col}:Q')]
            ).properties(height=250)
            return chart

        with chart1:
            st.altair_chart(make_chart(weather_data_df, 'temperature_degC', 'Temperature (℃)'), use_container_width=True)
        with chart2:
            st.altair_chart(make_chart(weather_data_df, 'dewpoint_degC', 'Dew Point (℃)'), use_container_width=True)
        with chart3:
            st.altair_chart(make_chart(weather_data_df, 'wind_speed_kmh', 'Wind Speed (kmh)'), use_container_width=True)
        with chart4:
            st.altair_chart(make_chart(weather_data_df, 'wind_gust_kmh', 'Wind Gust (kmh)'), use_container_width=True)
        with chart5:
            st.altair_chart(make_chart(weather_data_df, 'barometric_pressure_Pa', 'Barometric Pressure (Pa)'), use_container_width=True)
        with chart6:
            st.altair_chart(make_chart(weather_data_df, 'sea_level_pressure_Pa', 'Sea Level Pressure (Pa)'), use_container_width=True)
        with chart7:
            st.altair_chart(make_chart(weather_data_df, 'visibility_m', 'Visibility (m)'), use_container_width=True)
        with chart8:
            st.altair_chart(make_chart(weather_data_df, 'relative_humidity_percent', 'Relative Humidity %'), use_container_width=True)
        with chart9:
            st.altair_chart(make_chart(weather_data_df, 'wind_direction_deg', 'Wind Direction (deg)'), use_container_width=True)
        with chart10:
            st.altair_chart(make_chart(weather_data_df, 'precipitation_3h_mm', 'Precipitation (mm)'), use_container_width=True)
except Exception as e:
    st.error(f"Failed to load weather data table: {e}")