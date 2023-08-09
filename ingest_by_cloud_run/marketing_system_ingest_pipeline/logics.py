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
        # lst = ["OBJECT_FINALIZE"]
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
        object_id = "metadata/cost/mintegral/hours/2023/08/2023-08-01/12/1691486724916_acc1.json"

        if object_id.split("/")[0] not in (["metrics", "metadata"]):
            print("MESSAGE NOT MODULE INGEST")
            return "MESSAGE IN VALID"

        info = object_id.split("/")

        if info[3] == 'hours':
            export_date = info[6]
            hours = info[7]
        elif info[3] == 'day':
            export_date = info[6]
            hours = None
        else:
            export_date = None
            hours = None
        params = {
            'project_id': self.env_config['gcp_project_id'],
            'bucket_id': self.env_config['bucket_id'],
            'object_id': object_id,
            'file_type': info[0],
            'flow': info[1],
            'channel': info[2],
            'level': info[3],
            'year': info[4],
            'month': info[5],
            'export_date': export_date,
            'hours': hours,
            'timestamp': info[-1].split("_")[0]
        }





