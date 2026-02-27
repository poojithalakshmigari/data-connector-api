from flask import Flask, request, jsonify
import requests
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)


@app.route("/", methods=["GET"])
def health_check():
    return jsonify({
        "status": "running",
        "message": "Data Connector API is live"
    }), 200


@app.route("/ingest", methods=["POST"])
def ingest_data():
    if not request.is_json:
        return jsonify({
            "status": "error",
            "message": "Request must be JSON"
        }), 400

    data = request.get_json()

    if not data:
        return jsonify({
            "status": "error",
            "message": "Empty JSON payload"
        }), 400

    logging.info("Data received successfully")

    return jsonify({
        "status": "success",
        "records_received": len(data) if isinstance(data, list) else 1
    }), 200


@app.route("/fetch", methods=["GET"])
def fetch_external_data():
    url = request.args.get("url")

    if not url:
        return jsonify({
            "status": "error",
            "message": "URL parameter is required"
        }), 400

    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()

        return jsonify({
            "status": "success",
            "external_data": response.json()
        }), 200

    except requests.exceptions.RequestException as e:
        logging.error(str(e))
        return jsonify({
            "status": "error",
            "message": "Failed to fetch external API"
        }), 500


if __name__ == "__main__":
    app.run()
