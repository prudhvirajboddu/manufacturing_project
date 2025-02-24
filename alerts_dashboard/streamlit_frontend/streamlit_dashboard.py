import streamlit as st
import requests
import time

FLASK_API_URL = "http://backend:5000/alerts"

st.title("Factory Real-Time Alert Dashboard")

def get_alerts():
    try:
        response = requests.get(FLASK_API_URL)
        if response.status_code == 200:
            return response.json()
        return []
    except:
        return []

alert_placeholder = st.empty()

while True:
    alerts = get_alerts()
    if alerts:
        alert_placeholder.markdown("### Active Alerts")
        for alert in reversed(alerts[-5:]):
            st.warning(f"**{alert['timestamp']} - {alert['machine_id']}**: {alert['message']}\n"
                       f"Temp: {alert['temperature']}°C | Vibration: {alert['vibration']} | Pressure: {alert['pressure']}")
    time.sleep(2)
