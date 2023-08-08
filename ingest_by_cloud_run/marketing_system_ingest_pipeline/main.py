from enum import Enum
import traceback
from io import StringIO

import pandas as pd
from datetime import datetime, timedelta, timezone
import pytz
from commons.google_cloud_pubsub import *
from commons.Utils import *
from commons.google_cloud_bigquery import *
from google.cloud import bigquery
from commons.google_cloud_storage import *
from datetime import datetime
import json
from pytz import timezone

credentials_path = "key/apero-data-warehouse-connector.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
#
object_id = 'metadata/hours/mintegral-cost/2023/08/2023-08-02/09/1691486724916/metadata-info.json'
# object_id = 'metadata/day/mintegral-cost/2023/08/2023-08-02/1691486724916/metadata-info.json'
# object_id = 'metadata/month/mintegral-cost/2023/08/1691486724916/metadata-info.json'
#
# object_id = 'file_type/level/channel/year/month/day/hours/timestamp/metadata-info.json'
# object_id = 'file_type/level/channel/year/month/day/timestamp/metadata-info.json'
# object_id = 'file_type/level/channel/year/month/timestamp/metadata-info.json'


info = object_id.split("/")
file_type = info[0]
level = info[1]
channel = info[2]
year = info[3]
month = info[4]
timestamp = info[-2]

if level == 'hours':
    export_date = info[5]
    hours = info[6]
elif level == 'day':
    export_date = info[5]

list_cols = object_id.split("/")

client = storage.Client(project="apero-data-warehouse")
bucket = client.get_bucket("apero-marketing-raw")
blob = bucket.blob(object_id)

json_content = blob.download_as_text()
data = json.loads(json_content)
metadata = data['data']
df = pd.DataFrame()
for campaignInfo in metadata:
    df1 = pd.DataFrame(campaignInfo.values())
    df = pd.concat([df,df1])
print(df)

df['channel'] = list_cols[2]
df['export_date'] = list_cols[3]
df['export_hours'] = list_cols[4]
df['export_timestamp'] = list_cols[5]
print(df.columns)
# res_data = StringIO(df)
# res_df = pd.read_csv(res_data, sep="\t")
# print(res_df.info())

