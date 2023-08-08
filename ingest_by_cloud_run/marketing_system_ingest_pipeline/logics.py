from enum import Enum
import traceback

import pandas as pd
from datetime import datetime, timedelta, timezone
import pytz
from commons.google_cloud_pubsub import *
from commons.Utils import *
from commons.google_cloud_bigquery import *
from google.cloud import bigquery
from commons.google_cloud_storage import *
from datetime import datetime
from pytz import timezone

class PartitionField(Enum):
    EXPORT_DATE = 'export_date'
    EVENT_TIME = "table_export_date"

    def __str__(self):
        return self.value


class ClusterField(Enum):
    APP_NAME = 'app_name'

    def __str__(self):
        return self.value


def get_current_time():
    return datetime.strftime(
        datetime.now(timezone("Asia/Ho_Chi_Minh")),
        "%Y-%m-%d %H:%M:%S +0000"
    )

def convert_time(event_time):
    time_convert = event_time[0:10]
    return time_convert

class MarketingSystemPipeline(object):
    def __init__(self, logger, env):
        self.logger = logger
        config = load_config("config/marketing_system_config.yaml")
        self.env_config = config[env]
        self.client = bigquery.Client()


    def run(self):
        # event_type = pubsub_message['eventType']
        # lst = ["OBJECT_DELETE", "OBJECT_FINALIZE"]
        #
        # if event_type not in lst:
        #     print("MESSAGE IN VALID")
        #     return "MESSAGE IN VALID"
        #
        # print(pubsub_message)
        # print("START PROCESS MESSAGE REQUEST.")
        #
        # event_time = pubsub_message['eventTime']
        # time_convert = convert_time(event_time)
        # object_id = pubsub_message['objectId']
        object_id = 'metric/hour/2023-08-02/mintegral_old.json'

