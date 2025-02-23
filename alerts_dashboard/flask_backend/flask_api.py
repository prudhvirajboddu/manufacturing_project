from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

alerts = []

@app.route('/alert', methods=['POST'])
def receive_alert():
    data = request.json
    alerts.append(data)
    print(f"New Alert Received: {data}")
    return jsonify({"status": "Alert received!"})

@app.route('/alerts', methods=['GET'])
def get_alerts():
    return jsonify(alerts)

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)
