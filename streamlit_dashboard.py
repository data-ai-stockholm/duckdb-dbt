"""
Weather Data Analytics Dashboard using Streamlit.

Interactive dashboard for weather data visualization with:
- Summary statistics
- Temperature trends
- Weather conditions analysis
- Daily aggregates
- Anomaly detection results
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import duckdb
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="Weather Data Analytics",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom styling
st.markdown("""
    <style>
    [data-testid="stMetricValue"] {
        font-size: 2rem;
    }
    </style>
    """, unsafe_allow_html=True)


@st.cache_resource
def get_database_connection():
    """Get DuckDB connection."""
    return duckdb.connect("weather.duckdb")


def load_data(query):
    """Load data from DuckDB."""
    conn = get_database_connection()
    return conn.execute(query).df()


def main():
    """Main dashboard application."""

    # Header
    st.title("🌤️ Weather Data Analytics Dashboard")
    st.markdown("Real-time weather data visualization from DuckDB with dbt transformations")

    # Sidebar controls
    with st.sidebar:
        st.header("📊 Controls")

        selected_station = st.selectbox(
            "Select Station",
            options=["All Stations", "KJFK", "KLAX", "KORD", "KDFW", "KATL"]
        )

        metric_type = st.radio(
            "Select Metric",
            options=["Temperature", "Humidity", "Wind Speed", "Pressure"]
        )

        view_type = st.selectbox(
            "Select View",
            options=["Summary", "Trends", "Conditions", "Daily Aggregates", "Anomalies"]
        )

    # Load data
    try:
        # Get summary statistics
        if selected_station == "All Stations":
            summary_data = load_data("""
                SELECT
                    station_id,
                    location,
                    COUNT(*) as observations,
                    ROUND(AVG(temperature_degC), 2) as avg_temp_c,
                    ROUND(MIN(temperature_degC), 2) as min_temp_c,
                    ROUND(MAX(temperature_degC), 2) as max_temp_c,
                    ROUND(AVG(humidity_pct), 2) as avg_humidity,
                    ROUND(AVG(wind_speed_mps), 2) as avg_wind_mps,
                    ROUND(AVG(pressure_mb), 2) as avg_pressure
                FROM main_marts.fact_observations
                GROUP BY station_id, location
                ORDER BY observations DESC
            """)
        else:
            summary_data = load_data(f"""
                SELECT
                    station_id,
                    location,
                    COUNT(*) as observations,
                    ROUND(AVG(temperature_degC), 2) as avg_temp_c,
                    ROUND(MIN(temperature_degC), 2) as min_temp_c,
                    ROUND(MAX(temperature_degC), 2) as max_temp_c,
                    ROUND(AVG(humidity_pct), 2) as avg_humidity,
                    ROUND(AVG(wind_speed_mps), 2) as avg_wind_mps,
                    ROUND(AVG(pressure_mb), 2) as avg_pressure
                FROM main_marts.fact_observations
                WHERE station_id = '{selected_station}'
                GROUP BY station_id, location
            """)

        # SUMMARY VIEW
        if view_type == "Summary":
            st.header("📈 Summary Statistics")

            # Metrics row
            if len(summary_data) > 0:
                col1, col2, col3, col4, col5 = st.columns(5)

                with col1:
                    total_obs = summary_data["observations"].sum()
                    st.metric("📊 Total Observations", f"{total_obs:,}")

                with col2:
                    avg_temp = summary_data["avg_temp_c"].mean()
                    st.metric("🌡️ Avg Temperature", f"{avg_temp:.1f}°C")

                with col3:
                    min_temp = summary_data["min_temp_c"].min()
                    st.metric("❄️ Min Temperature", f"{min_temp:.1f}°C")

                with col4:
                    max_temp = summary_data["max_temp_c"].max()
                    st.metric("🔥 Max Temperature", f"{max_temp:.1f}°C")

                with col5:
                    avg_humidity = summary_data["avg_humidity"].mean()
                    st.metric("💧 Avg Humidity", f"{avg_humidity:.1f}%")

            # Detailed table
            st.subheader("Station Summary")
            st.dataframe(summary_data, use_container_width=True)

        # TRENDS VIEW
        elif view_type == "Trends":
            st.header("📈 Temperature Trends by Station")

            if selected_station == "All Stations":
                trends_data = load_data("""
                    SELECT
                        station_id,
                        observation_timestamp,
                        temperature_degC,
                        humidity_pct,
                        wind_speed_mps,
                        pressure_mb
                    FROM main_marts.fact_observations
                    ORDER BY observation_timestamp
                """)
            else:
                trends_data = load_data(f"""
                    SELECT
                        station_id,
                        observation_timestamp,
                        temperature_degC,
                        humidity_pct,
                        wind_speed_mps,
                        pressure_mb
                    FROM main_marts.fact_observations
                    WHERE station_id = '{selected_station}'
                    ORDER BY observation_timestamp
                """)

            if len(trends_data) > 0:
                # Temperature trend
                fig_temp = px.line(
                    trends_data,
                    x="observation_timestamp",
                    y="temperature_degC",
                    color="station_id",
                    title="Temperature Trends Over Time",
                    labels={"temperature_degC": "Temperature (°C)", "observation_timestamp": "Time"}
                )
                st.plotly_chart(fig_temp, use_container_width=True)

                # Other metrics
                col1, col2 = st.columns(2)

                with col1:
                    fig_humidity = px.line(
                        trends_data,
                        x="observation_timestamp",
                        y="humidity_pct",
                        color="station_id",
                        title="Humidity Trends",
                        labels={"humidity_pct": "Humidity (%)", "observation_timestamp": "Time"}
                    )
                    st.plotly_chart(fig_humidity, use_container_width=True)

                with col2:
                    fig_wind = px.line(
                        trends_data,
                        x="observation_timestamp",
                        y="wind_speed_mps",
                        color="station_id",
                        title="Wind Speed Trends",
                        labels={"wind_speed_mps": "Wind Speed (m/s)", "observation_timestamp": "Time"}
                    )
                    st.plotly_chart(fig_wind, use_container_width=True)

        # CONDITIONS VIEW
        elif view_type == "Conditions":
            st.header("🌤️ Weather Conditions Analysis")

            conditions_data = load_data("""
                SELECT
                    condition,
                    station_id,
                    COUNT(*) as count,
                    ROUND(AVG(temperature_degC), 2) as avg_temp
                FROM main_marts.fact_observations
                GROUP BY condition, station_id
                ORDER BY count DESC
            """)

            if len(conditions_data) > 0:
                col1, col2 = st.columns(2)

                with col1:
                    # Pie chart of conditions
                    condition_summary = conditions_data.groupby("condition")["count"].sum()
                    fig_pie = px.pie(
                        values=condition_summary.values,
                        names=condition_summary.index,
                        title="Weather Conditions Distribution"
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)

                with col2:
                    # Bar chart of conditions by station
                    fig_bar = px.bar(
                        conditions_data,
                        x="station_id",
                        y="count",
                        color="condition",
                        title="Conditions by Station",
                        labels={"count": "Observations", "station_id": "Station"}
                    )
                    st.plotly_chart(fig_bar, use_container_width=True)

                # Detailed table
                st.subheader("Conditions Detail")
                st.dataframe(conditions_data, use_container_width=True)

        # DAILY AGGREGATES VIEW
        elif view_type == "Daily Aggregates":
            st.header("📅 Daily Weather Aggregates")

            if selected_station == "All Stations":
                daily_data = load_data("""
                    SELECT
                        observation_date,
                        station_id,
                        location,
                        observation_count,
                        avg_temperature_degC,
                        min_temperature_degC,
                        max_temperature_degC,
                        avg_humidity_pct,
                        avg_wind_speed_mps,
                        max_wind_speed_mps,
                        avg_pressure_mb
                    FROM main_marts.fact_daily_weather
                    ORDER BY observation_date DESC
                """)
            else:
                daily_data = load_data(f"""
                    SELECT
                        observation_date,
                        station_id,
                        location,
                        observation_count,
                        avg_temperature_degC,
                        min_temperature_degC,
                        max_temperature_degC,
                        avg_humidity_pct,
                        avg_wind_speed_mps,
                        max_wind_speed_mps,
                        avg_pressure_mb
                    FROM main_marts.fact_daily_weather
                    WHERE station_id = '{selected_station}'
                    ORDER BY observation_date DESC
                """)

            if len(daily_data) > 0:
                # Daily temperature range
                fig_range = px.bar(
                    daily_data,
                    x="observation_date",
                    y=["max_temperature_degC", "min_temperature_degC"],
                    title="Daily Temperature Range by Station",
                    labels={"observation_date": "Date", "value": "Temperature (°C)"}
                )
                st.plotly_chart(fig_range, use_container_width=True)

                # Detailed table
                st.subheader("Daily Summary Table")
                st.dataframe(daily_data, use_container_width=True)

        # ANOMALIES VIEW
        elif view_type == "Anomalies":
            st.header("⚠️ Extreme Weather Events (Anomalies)")

            anomalies_data = load_data("""
                SELECT
                    observation_timestamp,
                    station_id,
                    temperature_degC,
                    ROUND(temp_z_score, 2) as z_score,
                    temperature_category
                FROM main_marts.extreme_weather_events
                ORDER BY observation_timestamp DESC
            """)

            if len(anomalies_data) > 0:
                # Category breakdown
                col1, col2, col3 = st.columns(3)

                with col1:
                    extreme_count = len(anomalies_data[anomalies_data["temperature_category"] == "Extreme"])
                    st.metric("🔥 Extreme Events", extreme_count)

                with col2:
                    unusual_count = len(anomalies_data[anomalies_data["temperature_category"] == "Unusual"])
                    st.metric("⚠️ Unusual Events", unusual_count)

                with col3:
                    total_anomalies = len(anomalies_data)
                    st.metric("📊 Total Anomalies", total_anomalies)

                # Scatter plot of anomalies
                fig_scatter = px.scatter(
                    anomalies_data,
                    x="observation_timestamp",
                    y="temperature_degC",
                    color="temperature_category",
                    size="z_score",
                    hover_data=["station_id", "z_score"],
                    title="Temperature Anomalies (Z-score Analysis)",
                    labels={"temperature_degC": "Temperature (°C)", "observation_timestamp": "Time"}
                )
                st.plotly_chart(fig_scatter, use_container_width=True)

                # Detailed table
                st.subheader("Anomaly Details")
                st.dataframe(anomalies_data, use_container_width=True)
            else:
                st.info("No anomalies detected in the data.")

    except Exception as e:
        st.error(f"❌ Error loading data: {str(e)}")
        st.info("Make sure to run `dbt run` first to create the tables.")

    # Footer
    st.divider()
    st.markdown("""
        <div style='text-align: center; color: gray; font-size: 12px;'>
        📊 Weather Data Analytics Dashboard | Powered by Streamlit, DuckDB, and dbt
        </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
