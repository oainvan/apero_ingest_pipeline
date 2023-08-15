from enum import Enum
import traceback

import pandas as pd
from datetime import datetime, timedelta, timezone
import pytz
from base.core_functions.Utils import *
from base.core_functions.google_cloud_bigquery import *

from datetime import datetime
from pytz import timezone


class PartitionField(Enum):
    EVENT_DATE = 'event_date'


def get_current_time():
    return datetime.strftime(
        datetime.now(timezone("Asia/Ho_Chi_Minh")),
        "%Y-%m-%d %H:%M:%S +0000"
    )


class RealTimeSubcriptions(object):
    def __init__(self, logger, env):
        self.logger = logger
        config = load_config("services/real_time_subcriptions/config.yaml")
        self.env_config = config[env]
        self.bq_config = config['bq_config']
        self.client = bigquery.Client()

    def run(self, pubsub_message):

        package_name = pubsub_message['packageName']
        event_timestamp = pubsub_message['eventTimeMillis']
        response = pubsub_message['subscriptionNotification']

        data = pd.DataFrame(response)
        data['package_name'] = package_name
        data['event_timestamp'] = event_timestamp

        data['event_date'] = pd.to_datetime(data['event_timestamp'], unit='ms')
        data['event_date'] = data['event_date'].dt.tz_localize('UTC').dt.tz_convert('Asia/Bangkok')
        data = data.reset_index(drop=True)

        upload_df_to_bigquery(
            self.logger, data, self.bq_config['output_table'],
            str(PartitionField.EVENT_DATE),
            package_name
        )



