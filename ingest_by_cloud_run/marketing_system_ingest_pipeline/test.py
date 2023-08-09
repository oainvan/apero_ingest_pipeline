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
# object_id = 'file_type/flow/channel/level/year/month/2023-08-01/12/1691486724916_acc1.json'
object_id = 'metrics/cost/mintegral/hours/2023/08/2023-08-01/12/1691486724916_acc1.json'
# object_id = "metadata/cost/mintegral/hours/2023/08/2023-08-01/12/1691486724916_acc1.json"
# object_id = 'metrics/cost/mintegral/hours/2023/08/2023-08-01/1691486724916_acc1.json'
# object_id = 'metrics/cost/mintegral/hours/2023/08/1691486724916_acc1.json'



info = object_id.split("/")
file_type = info[0]
flow = info[1]
channel = info[2]
level = info[3]
year = info[4]
month = info[5]
timestamp = info[-1].split("_")[0]

if level == 'hours':
    export_date = info[6]
    hours = info[7]
elif level == 'day':
    export_date = info[6]


client = storage.Client(project="apero-data-warehouse")
bucket = client.get_bucket("apero-marketing-raw")
blob = bucket.blob(object_id)
json_content = blob.download_as_text()
data = json.loads(json_content)['data']
# print(data)
df = pd.read_csv(StringIO(data), sep='\t')
print(df)

# if file_type == 'metadata':
#     data = json.loads(json_content)
#     metadata = data['data']
#     df = pd.DataFrame()
#     for campaignInfo in metadata:
#         df1 = pd.DataFrame(campaignInfo.values())
#         df = pd.concat([df,df1])
#
df['channel'] = channel
df['level'] = level
df['year'] = year
df['month'] = month
df['export_date'] = export_date
df['hours'] = hours
df['timestamp'] = timestamp
#
# df['export_date'] = pd.to_datetime(df['export_date'].astype(str), format="%Y-%m-%d").dt.date
#
# client = bigquery.Client()
# partitioning = bigquery.table.TimePartitioning(
#     type_=bigquery.TimePartitioningType.DAY,
#     field='export_date'  # Specify the field to use for partitioning
# )
# job_config = bigquery.LoadJobConfig(
#     write_disposition="WRITE_APPEND",
#     time_partitioning=partitioning
# )
# job_config.clustering_fields = 'channel'
# df = df.reset_index(drop=True)
# job = client.load_table_from_dataframe(
#     df, f"oai_dev_2.{file_type}_{channel}_{flow}_{level}", job_config=job_config
# )
# job.result()