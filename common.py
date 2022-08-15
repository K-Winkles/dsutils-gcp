from google.cloud import bigquery
import pandas as pd
import datetime
import json
import yaml
current_time = datetime.datetime.now()
bqclient = bigquery.Client()

def get_config(filepath):
    config = open(filepath, 'r')
    content = yaml.safe_load(config)
    return content

def get_table_data(bq_table_spec):
    table = bigquery.TableReference.from_string(bq_table_spec)
    rows = bqclient.list_rows(table)
    data = rows.to_dataframe()
    return data

def get_bucket_data(gsutil_uri):
    if '.csv' in gsutil_uri:
        data = pd.read_csv(gsutil_uri)
    elif '.json' in gsutil_uri:
        data = pd.read_json(gsutil_uri)
    else:
        return 'filetype invalid'
    return data

def parse_data(source_data, target_data, report, mode):
    bool_compare_row_count = compare_row_count(source_data, target_data, report)
    bool_compare_schema = compare_schema(source_data, target_data,  report, mode)
    bool_check_duplicates_source = check_duplicates(source_data, report)
    bool_check_duplicates_target = check_duplicates(target_data, report)
    bool_compare_diff = compare_diff(source_data, target_data, report)
    if (bool_compare_row_count and 
        bool_compare_schema and 
        bool_check_duplicates_source and
        bool_check_duplicates_target and
        bool_compare_diff):
        print('all matched')
        return True 
    else: 
        print('anomalies found')
        return False

def compare_row_count(source_data, target_data, report):
    string_to_write = '---ROW CHECK---\nsource data rows: {}\ntarget data rows:{}\n'.format(len(source_data), len(target_data))
    if len(source_data) == len(target_data):
        string_to_write = string_to_write + 'remarks: PASSED\n'
        report.write(string_to_write)
        return True
    else:
        string_to_write = string_to_write + 'remarks: FAILED\n'
        report.write(string_to_write)
        return False

def compare_schema(source_data, target_data, report, mode):
    string_to_write = '---SCHEMA CHECK---\nmode: {}\n'.format(mode)
    source_schema = {}

    for i in range(len(source_data.columns)):
        source_schema[source_data.columns.tolist()[i]] = str(source_data.dtypes[i])

    target_schema = {}
    for i in range(len(target_data.columns)):
        target_schema[target_data.columns.tolist()[i]] = str(target_data.dtypes[i])

    string_to_write = string_to_write + 'source schema: {}\ntarget schema: {}\n'.format(json.dumps(source_schema), json.dumps(target_schema))

    if mode =='strict' and source_schema == target_schema:
        string_to_write = string_to_write + 'remarks: PASSED\n'
        report.write(string_to_write)
        return True
    elif mode == 'default' and source_schema.keys() == target_schema.keys():
        string_to_write = string_to_write + 'remarks: PASSED\n'
        report.write(string_to_write)
        return True
    else: 
        string_to_write = string_to_write + 'remarks: FAILED\n'
        report.write(string_to_write)
        return False


def compare_diff(source_data, target_data, report):
    source_data_columns = source_data.columns.tolist()
    target_data_columns = target_data.columns.tolist()
    sorted_source_data = source_data.sort_values(by=source_data_columns, ignore_index=True)
    sorted_target_data = target_data.sort_values(by=target_data_columns, ignore_index=True)
    comparison_df = sorted_source_data.compare(sorted_target_data, keep_shape=True, keep_equal=True)
    string_to_write = '---DIFF CHECK---\n{}\n'.format(comparison_df.to_string())
    
    if comparison_df.isnull().values.any():
        string_to_write = string_to_write + 'remarks: FAILED\n'
        report.write(string_to_write)
        return False
    else:
        string_to_write = string_to_write + 'remarks: PASSED\n'
        report.write(string_to_write)
        return True

def check_duplicates(data, report):
    duplicate_check_df = data.duplicated()
    number_of_duplicates = duplicate_check_df.sum()
    duplicate_rows = data.loc[data.duplicated(), :]
    string_to_write = '---DUPLICATE CHECK---\n'

    if number_of_duplicates > 0:
        string_to_write = string_to_write + duplicate_rows.to_string() + '\nremarks: FAILED\n'
        report.write(string_to_write)
        return False
    else:
        string_to_write = string_to_write + 'remarks:PASSED\n'
        report.write(string_to_write)
        return True

def get_sample_values(data):
    return data.head(10)