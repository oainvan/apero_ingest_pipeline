import logging
import os
import traceback
import json
import google.cloud.logging
from flask import Flask, request
from datetime import datetime
import base64
from pytz import timezone
from services.real_time_subcriptions.logics import RealTimeSubcriptions
from base.core_functions.Utils import *

google_logging_client = google.cloud.logging.Client()
google_logging_client.setup_logging()
logger = logging

app = Flask(__name__)


@app.route('/', methods=["POST"])
def index():
    envelope = request.get_json()
    logger.info("Import workflow - Rec: {msg}".format(msg=str(envelope)))
    if not envelope:
        msg = "No Pub/Sub message received"
        logger.error(f"Error: {msg}")
        return f"Bad Request: {msg}", 204

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "Invalid Pub/Sub message format"
        logger.error(f"Error: {msg}")
        return f"Bad Request: {msg}", 204

    data = envelope["message"]["data"]
    decoded_bytes = base64.b64decode(data)
    decoded_string = decoded_bytes.decode('utf-8')
    print(decoded_string)

    pubsub_message = json.loads(decoded_string)

    if 'subscriptionNotification' not in pubsub_message:
        print(f"PubSub Message: {pubsub_message}")
        return "Message Invalid", 204

    run_env = "product"
    try:
        mkt_ingest = RealTimeSubcriptions(
            logger=logger,
            env=run_env
        )
        mkt_ingest.run(pubsub_message)

    except Exception as ex:
        logger.error("Exception: {msg}".format(msg=traceback.format_exc()))
        send_except_alert(
            run_env,
            "Exception: {msg}\n Message: {mes}".format(msg=ex, mes=pubsub_message),
            "Apero-TrustedApp Real Time Subcriptions",
            "services/real_time_subcriptions/config.yaml"
        )
        return "", 403

    return "Run Successfully", 200


if __name__ == '__main__':
    PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 8080
    app.run(host="127.0.0.1", port=PORT, debug=True)
