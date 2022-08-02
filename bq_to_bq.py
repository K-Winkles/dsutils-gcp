from google.cloud import bigquery
import sys
import pandas as pd

bqclient = bigquery.Client()

def get_table_data(bq_table_spec):
    table = bigquery.TableReference.from_string(bq_table_spec)
    rows = bqclient.list_rows(table)
    data = rows.to_dataframe()
    return data

def parse_data(source_data, target_data):
    if compare_row_count(source_data, target_data) and compare_schema(source_data, target_data) and compare_diff(source_data, target_data):
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

def compare_schema(source_data, target_data):
    source_schema = {}
    for i in range(len(source_data.dtypes.tolist())):
        source_schema[source_data.columns.tolist()[i]] = source_data.dtypes.tolist()[i]

    target_schema = {}
    for i in range(len(target_data.dtypes.tolist())):
        target_schema[target_data.columns.tolist()[i]] = target_data.dtypes.tolist()[i]

    if source_schema == target_schema:
        return True
    else: 
        print('anomaly detected in schemas')
        return False


def compare_diff(source_data, target_data):
    comparison_df = source_data.compare(target_data)
    if comparison_df.isnull().values.any():
        print('anomaly detected in diff')
        return False
    else:
        return True

def get_sample_values(data):
    return data.head(10)

if __name__ in '__main__':
    cli_arguments = sys.argv
    bq_table_spec_source = cli_arguments[1]
    bq_table_spec_target = cli_arguments[2]

    source_data = get_table_data(bq_table_spec_source)
    target_data = get_table_data(bq_table_spec_target)
    results = parse_data(source_data, target_data)
