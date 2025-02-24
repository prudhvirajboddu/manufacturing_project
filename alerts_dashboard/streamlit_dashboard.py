import time
import pandas as pd
import pymongo
import streamlit as st
import plotly.express as px
import uuid  # To generate unique keys

# MongoDB connection details
MONGO_URI = "mongodb://mongodb:27017"
DATABASE_NAME = "manufacturing"
COLLECTION_NAME = "sensor_data"

# Connect to MongoDB
client = pymongo.MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# Streamlit Page Config
st.set_page_config(page_title="Real-Time Sensor Dashboard", layout="wide")

st.title("Real-Time Manufacturing Sensor Dashboard")

# Sidebar - Machine Selection
st.sidebar.header("Filter Options")
machine_ids = collection.distinct("machine_id")
selected_machine = st.sidebar.selectbox("Select Machine", machine_ids)

# Thresholds for alerts
TEMP_THRESHOLD = 85
VIBRATION_THRESHOLD = 0.005
ERROR_CODES_TO_ALERT = [1, 2, 3]

# Track previous alerts to avoid duplicates
alerted_sensors = set()


def fetch_latest_data():
    """Fetches the latest sensor data from MongoDB"""
    cursor = collection.find({"machine_id": selected_machine}).sort("timestamp", -1).limit(100)
    data = list(cursor)

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Ensure timestamp is in datetime format
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.drop(columns=['_id'], inplace=True, errors="ignore")  # Drop MongoDB ObjectId

    return df


placeholder = st.empty()  # Placeholder for real-time updates

while True:
    df = fetch_latest_data()

    if not df.empty:
        with placeholder.container():
            col1, col2 = st.columns(2)

            # Generate a unique key for each refresh
            unique_id = str(uuid.uuid4())  

            # Temperature Trend (Unique Key)
            fig_temp = px.line(df, x='timestamp', y='temperature', title=f"Temperature Trend for {selected_machine}")
            col1.plotly_chart(fig_temp, use_container_width=True, key=f"temp_chart_{unique_id}")

            # Vibration Trend (Unique Key)
            fig_vibration = px.line(df, x='timestamp', y='vibration', title=f"Vibration Levels for {selected_machine}")
            col2.plotly_chart(fig_vibration, use_container_width=True, key=f"vibration_chart_{unique_id}")

            # Pressure Distribution (Unique Key)
            fig_pressure = px.histogram(df, x='pressure', title=f"Pressure Distribution for {selected_machine}", nbins=20)
            st.plotly_chart(fig_pressure, use_container_width=True, key=f"pressure_chart_{unique_id}")

            # Error Code Analysis (Unique Key)
            error_counts = df['error_code'].value_counts().reset_index()
            error_counts.columns = ['Error Code', 'Count']
            fig_errors = px.bar(error_counts, x='Error Code', y='Count', title=f"Error Codes for {selected_machine}")
            st.plotly_chart(fig_errors, use_container_width=True, key=f"error_chart_{unique_id}")

            # Summary Metrics
            st.sidebar.write("Summary Metrics")
            st.sidebar.metric(label="Max Temperature", value=f"{df['temperature'].max()}°C")
            st.sidebar.metric(label="Min Temperature", value=f"{df['temperature'].min()}°C")
            st.sidebar.metric(label="Average Pressure", value=f"{df['pressure'].mean()} bar")
            st.sidebar.metric(label="Total Error Events", value=df[df['error_code'] > 0].shape[0])

            # Real-Time Alerts
            latest_record = df.iloc[0]  # Get the most recent record

            alert_message = None

            if latest_record["temperature"] > TEMP_THRESHOLD:
                alert_message = f"High Temperature Alert! ({latest_record['temperature']}°C)"
            elif latest_record["vibration"] > VIBRATION_THRESHOLD:
                alert_message = f"High Vibration Alert! ({latest_record['vibration']})"
            elif latest_record["error_code"] in ERROR_CODES_TO_ALERT:
                alert_message = f"Error Code Detected: {latest_record['error_code']}"

            if alert_message and latest_record["machine_id"] not in alerted_sensors:
                st.toast(alert_message)
                st.sidebar.error(alert_message)
                alerted_sensors.add(latest_record["machine_id"])

                
    time.sleep(5)  # Refresh every 5 seconds
