from enum import Enum
import traceback
from io import StringIO

import pandas as pd
from datetime import datetime, timedelta, timezone
import pytz
from commons.google_cloud_pubsub import *
from commons.Utils import *
from logics import MarketingSystemPipeline
from commons.google_cloud_bigquery import *
from google.cloud import bigquery
from commons.google_cloud_storage import *
from datetime import datetime
import json
import time
from pytz import timezone
from commons.common_functions import *

credentials_path = "key/apero-data-warehouse-connector.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

# str = "metadata/revenue/admob/hours/2023/08/2023-08-11/16/1691659319765_c72eceec-ecee-4939-9a0a-aad9f34e29ed.json"
# str1 = "metrics/cost/mintegral/hours/2023/08/2023-08-11/16/1691661049335_107a645d-9da7-4630-89f2-329ca91ffb29.json"
str2 = "metrics/revenue/admob/hours/2023/08/2023-08-10/17/1691663400009_c72eceec-ecee-4939-9a0a-aad9f4e29.json"
attributes= {
    "bucketId": "apero-marketing-raw",
    "eventTime": "2023-08-10T09:34:06.928335Z",
    "eventType": "OBJECT_FINALIZE",
    "notificationConfig": "projects/_/buckets/apero-marketing-raw/notificationConfigs/5",
    "objectGeneration": "1691660046922860",
    "objectId": str2,
    "payloadFormat": "JSON_API_V1"
  }

def get_cur_date():
    return datetime.now(timezone("Asia/Ho_Chi_Minh"))
def g_logger(run_date, folder):
    """
    Create logging.
    :param run_date:
    :param folder:
    :return:
    """
    logger = get_logger(folder, run_date.date(), run_date.hour)
    return logger
run_date = get_cur_date()
logger = g_logger(run_date, "Ingest_Marketing_Raw")

upload = MarketingSystemPipeline(logger, 'product')
print(time.time())
upload.run(attributes)
print(time.time())
