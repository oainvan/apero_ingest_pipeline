from commons.google_cloud_storage import *
from commons.Utils import *
import json
import pandas as pd
from io import StringIO
import timeit

def process_response_data(**params):
    if params['file_type'] not in ['metadata', 'metrics']:
        return 'file_type_invalid'
    client = storage.Client(project=params['project_id'])
    bucket = client.get_bucket(params['bucket_id'])
    blob = bucket.blob(params['object_id'])
    json_content = blob.download_as_text()
    data = json.loads(json_content)['data']

    raw_result = process_json_data(data, **params)
    if raw_result.empty:
        return 'raw_result_empty'
    list_col = [x.lower().replace(' ', '_') for x in raw_result.columns]
    raw_result.columns = list_col
    list_col_add = ['flow', 'channel', 'level', 'year', 'month', 'export_date', 'hours', 'timestamp', 'adsAccountId']
    for col in list_col_add:
        raw_result[col] = params[col]

    raw_result['export_date'] = pd.to_datetime(raw_result['export_date'], format="%Y-%m-%d").dt.date
    raw_result = raw_result.reset_index(drop=True)
    if 'daily_budget' in raw_result.columns:
        raw_result['daily_budget'] = raw_result['daily_budget'].astype(float)
    return raw_result


def process_json_data(data, **params):
    """
    Process raw data after json load.
    :param data:
    :return:
    """
    raw_result = pd.DataFrame()
    if params['file_type'] == 'metrics' and params['channel'] == 'admob':
        data = data[:-1]
        lst = []
        for campaignInfo in data[1:]:
            flattened_data = convert_data_to_df(campaignInfo)
            lst.append(flattened_data)
        raw_result = pd.DataFrame(lst)
    else:
        if isinstance(data, str):
            raw_result = pd.read_csv(StringIO(data), sep='\t')
        elif isinstance(data, dict):
            raw_result = pd.DataFrame([data])
        elif isinstance(data, list):
            raw_result = pd.DataFrame(data)
    return raw_result

def convert_data_to_df(input_data):
    converted_data = {}

    for key, value in input_data['row']['dimensionValues'].items():
        converted_data[f'dimension_{key}'] = value['value']
        if 'displayLabel' in value:
            converted_data[f'displayLabel_{key}'] = value['displayLabel']

    for key, value in input_data['row']['metricValues'].items():
        converted_data[f'metric_{key}'] = value['microsValue'] if 'microsValue' in value else value['integerValue']
    return converted_data

def get_params(object_id, env_config):
    info = object_id.split("/")

    if info[3] == 'hours':
        export_date = info[6]
        hours = info[7]
    elif info[3] == 'days':
        export_date = info[6]
        hours = None
    elif info[3] == 'months':
        export_date = None
        hours = None
    else:
        return 'Level invalid'

    params = {
        'project_id': env_config['gcp_project_id'],
        'bucket_id': env_config['bucket_id'],
        'object_id': object_id,
        'file_type': info[0],
        'flow': info[1],
        'channel': info[2],
        'level': info[3],
        'year': info[4],
        'month': info[5],
        'export_date': export_date,
        'hours': hours,
        'timestamp': info[-1].split("_")[0],
        'adsAccountId': info[-1].split("_")[-1]
    }
    return params