from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

@app.route("/")
def home():
    return "Data Connector API is running"

@app.route("/ingest", methods=["POST"])
def ingest_data():
    data = request.json
    return jsonify({
        "status": "success",
        "received_data": data
    })

@app.route("/fetch", methods=["GET"])
def fetch_external_data():
    url = request.args.get("url")
    try:
        response = requests.get(url)
        return jsonify({
            "status": "success",
            "data": response.json()
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        })

if __name__ == "__main__":
    app.run(debug=True)
