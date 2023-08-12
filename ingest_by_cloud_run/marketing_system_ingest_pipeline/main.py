import logging
import os
import traceback

import google.cloud.logging
from flask import Flask, request
from logics import MarketingSystemPipeline
from datetime import datetime
from pytz import timezone
from commons.Utils import *

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

    pubsub_message = envelope["message"]["attributes"]

    if 'eventType' not in pubsub_message:
        return "Message Invalid", 204

    run_env = "product"
    print(pubsub_message)
    try:
        mkt_ingest = MarketingSystemPipeline(
            logger=logger,
            env=run_env
        )
        mkt_ingest.run(pubsub_message)

    except Exception as ex:
        logger.error("Exception: {msg}".format(msg=traceback.format_exc()))
        send_except_alert(
            run_env,
            "Exception: {msg}\n Message: {mes}".format(msg=ex, mes=pubsub_message),
            "Marketing System Ingest Pipeline"
        )
        return "", 204

    return "Run Successfully", 200


if __name__ == '__main__':
    PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 8080
    app.run(host="127.0.0.1", port=PORT, debug=True)
