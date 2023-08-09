from google_cloud_pubsub import *
from google_cloud_pubsub import *
from google_cloud_storage import *
from Utils import *
import json
import pandas as pd
from io import StringIO


def mintegral_process(**params):
    if params['file_type'] not in ['metadata', 'metrics']:
        return 'file_type_invalid'
    client = storage.Client(project=params['project_id'])
    bucket = client.get_bucket(params['bucket_id'])
    blob = bucket.blob(params['object_id'])
    json_content = blob.download_as_text()
    data = json.loads(json_content)['data']

    raw_result = pd.DataFrame()
    if params['file_type'] == 'metadata':
        for campaignInfo in data:
            df = pd.DataFrame(campaignInfo.values())
            raw_result = pd.concat([raw_result, df])
    elif params['file_type'] == 'metrics':
        raw_result = pd.read_csv(StringIO(data), sep='\t')

    list_col = [x.lower().replace(' ', '_') for x in raw_result.columns]
    raw_result.columns = list_col

    list_col_add = ['channel', 'level', 'year', 'month', 'export_date', 'hours', 'timestamp']

    for col in list_col_add:
        raw_result[col] = params[col]

    return raw_result
