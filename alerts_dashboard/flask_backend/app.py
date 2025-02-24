import time
import json
import requests
from pymongo import MongoClient
from flask import Flask, request, jsonify
from flask_cors import CORS

# Initialize Flask App
app = Flask(__name__)
CORS(app)

# MongoDB Connection
MONGO_URI = "mongodb://mongodb:27017/"
DATABASE_NAME = "manufacturing"
ALERTS_COLLECTION = "alerts"
SENSOR_COLLECTION = "sensor_data"

# Flask API URL (inside Docker)
FLASK_API_URL = "http://backend:5000/alert"

# Define Sensor Thresholds
TEMP_THRESHOLD = 80.0
VIBRATION_THRESHOLD = 0.007
PRESSURE_THRESHOLD = 1.5

try:
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    alerts_collection = db[ALERTS_COLLECTION]
    sensor_collection = db[SENSOR_COLLECTION]
    print(" Successfully connected to MongoDB!")
except Exception as e:
    print(f" Error connecting to MongoDB: {e}")
    exit(1)

@app.route('/alert', methods=['POST'])
def receive_alert():
    try:
        data = request.json
        alerts_collection.insert_one(data)
        print(f" New Alert Received: {data}")
        return jsonify({"status": "Alert stored in MongoDB"})
    except Exception as e:
        return jsonify({"error": f"Failed to store alert: {e}"}), 500

@app.route('/alerts', methods=['GET'])
def get_alerts():
    try:
        alerts = list(alerts_collection.find({}, {"_id": 0}))
        return jsonify(alerts)
    except Exception as e:
        return jsonify({"error": f"Error retrieving alerts: {e}"}), 500

def monitor_sensors():
    """
    Monitors latest sensor readings from MongoDB, detects anomalies,
    and sends alerts to Flask API.
    """
    while True:
        try:
            latest_record = sensor_collection.find().sort("timestamp", -1).limit(1)

            for record in latest_record:
                temperature = record.get("temperature", 0)
                vibration = record.get("vibration", 0)
                pressure = record.get("pressure", 0)

                if (temperature > TEMP_THRESHOLD or
                    vibration > VIBRATION_THRESHOLD or
                    pressure > PRESSURE_THRESHOLD):

                    alert_data = {
                        "timestamp": record["timestamp"],
                        "machine_id": record["machine_id"],
                        "temperature": temperature,
                        "vibration": vibration,
                        "pressure": pressure,
                        "message": "⚠ ALERT: Abnormal sensor reading detected!"
                    }

                    print(f" Sending Alert: {alert_data}")
                    try:
                        response = requests.post(FLASK_API_URL, json=alert_data, timeout=5)
                        if response.status_code == 200:
                            print(" Alert successfully sent to Flask API!")
                        else:
                            print(f"Failed to send alert. HTTP {response.status_code}")
                    except requests.exceptions.RequestException as e:
                        print(f"Error sending alert to Flask API: {e}")

        except Exception as e:
            print(f"Error fetching sensor data: {e}")

        time.sleep(2)

if __name__ == '__main__':
    print("🔍 Monitoring sensor data for anomalies...")
    from threading import Thread
    Thread(target=monitor_sensors).start()
    app.run(debug=True, host="0.0.0.0", port=5000)
