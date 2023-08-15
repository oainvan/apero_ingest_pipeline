from enum import Enum
import traceback

import pandas as pd
from datetime import datetime, timedelta, timezone
import pytz
from commons.google_cloud_pubsub import *
from commons.Utils import *
from commons.google_cloud_bigquery import *
from google.cloud import bigquery
from commons.common_functions import *
from commons.google_cloud_storage import *
from datetime import datetime
from pytz import timezone

class PartitionField(Enum):
    EXPORT_DATE = 'export_date'
    EVENT_TIME = "table_export_date"

    def __str__(self):
        return self.value


class ClusterField(Enum):
    HOURS = 'hours'

    def __str__(self):
        return self.value


def get_current_time():
    return datetime.strftime(
        datetime.now(timezone("Asia/Ho_Chi_Minh")),
        "%Y-%m-%d %H:%M:%S +0000"
    )


class MarketingSystemPipeline(object):
    def __init__(self, logger, env):
        self.logger = logger
        config = load_config("config/marketing_system_config.yaml")
        self.env_config = config[env]
        self.bq_config = config['bq_config']
        self.client = bigquery.Client()

    def run(self, pubsub_message):
        """
        Run functions.
        :param pubsub_message:
        :return:
        """
        # Check event_type
        event_type = pubsub_message['eventType']
        lst = ["OBJECT_FINALIZE"]
        if event_type not in lst:
            print("MESSAGE IN VALID")
            return "MESSAGE IN VALID"

        # Check object_id
        object_id = pubsub_message['objectId']
        if object_id.split("/")[0] not in (["metrics", "metadata"]):
            print("MESSAGE NOT MODULE INGEST")
            return "MESSAGE IN VALID"

        print(pubsub_message)
        print("START PROCESS MESSAGE REQUEST.")
        params = get_params(object_id, self.env_config)
        self.logger.info(f"Params: {params}")

        # print(params)
        raw_result = process_response_data(**params)
        self.upload_data_crawl(
            raw_result, str(PartitionField.EXPORT_DATE),
            str(ClusterField.HOURS), **params
        )

    def upload_data_crawl(self, raw_result, partition, cluster, **params):
        """
        Upload data crawled to bigquery
        :param cluster:
        :param partition:
        :param raw_result:
        :param params:
        :return:
        """
        table_upload = '{}.{}.{}'.format(
            self.env_config['gcp_project_id'],
            self.bq_config['dataset'][params['file_type']],
            self.bq_config['table']['table_upload'].format(
                channel=params['channel'],
                flow=params['flow'],
                level=params['level'],
                file_type=params['file_type']
            )
        )
        if params['level'] == 'hours':
            self.logger.info(f'Start Upload {params["object_id"]} to {table_upload}')
            upload_df_to_bigquery(self.logger, raw_result, table_upload, partition, cluster)
        elif params['level'] == 'days':
            self.logger.info(f'Start Upload {params["object_id"]} to {table_upload}')
            upload_df_to_bigquery(self.logger, raw_result, table_upload, partition, None)
        elif params['level'] == 'months':
            self.logger.info(f'Start Upload {params["object_id"]} to {table_upload}')
            upload_df_to_bigquery(self.logger, raw_result, table_upload, None, None)
        else:
            self.logger.warning("Level invalid to call upload bigquery functions")
