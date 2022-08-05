from google.cloud import bigquery
import sys
import pandas as pd

bqclient = bigquery.Client()

def get_data(gsutil_uri):
    if '.csv' in gsutil_uri:
        data = pd.read_csv(gsutil_uri)
    elif '.json' in gsutil_uri:
        data = pd.read_json(gsutil_uri)
    else:
        return 'filetype invalid'
    return data

def parse_data(source_data, target_data, mode):
    if compare_row_count(source_data, target_data) and compare_schema(source_data, target_data, mode) and compare_diff(source_data, target_data):
        print('all matched')
        return True 
    else: 
        return False

def compare_row_count(source_data, target_data):
    if len(source_data) == len(target_data):
        return True
    else:
        print('anomaly detected in row count')
        return False

def compare_schema(source_data, target_data, mode):
    source_schema = {}
    for i in range(len(source_data.dtypes.tolist())):
        source_schema[source_data.columns.tolist()[i]] = source_data.dtypes.tolist()[i]

    target_schema = {}
    for i in range(len(target_data.dtypes.tolist())):
        target_schema[target_data.columns.tolist()[i]] = target_data.dtypes.tolist()[i]

    if mode =='strict' and source_schema == target_schema:
        return True
    elif mode == 'default' and source_schema.keys() == target_schema.keys():
        return True
    else: 
        print('anomaly detected in schemas')
        return False


def compare_diff(source_data, target_data):
    comparison_df = source_data.compare(target_data, keep_shape=True, keep_equal=True)
    if comparison_df.isnull().values.any():
        print('anomaly detected in diff')
        return False
    else:
        return True

def get_sample_values(data):
    return data.head(10)

if __name__ in '__main__':
    cli_arguments = sys.argv
    gsutil_uri_source = cli_arguments[1]
    gsutil_uri_target = cli_arguments[2]

    source_data = get_data(gsutil_uri_source)
    target_data = get_data(gsutil_uri_target)
    results = parse_data(source_data, target_data, mode='default')
