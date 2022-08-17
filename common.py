import os
import pathlib

from google.cloud import bigquery
import pandas as pd
import datetime
import json
import logging
import yaml

current_time = datetime.datetime.now()
current_folder_name = f'{current_time.strftime("%m-%d-%Y")}/{current_time.strftime("%H.%M.%S")}'

pathlib.Path(f'logs/{current_folder_name}').mkdir(parents=True, exist_ok=True)

error_log_filename = f'logs/{current_folder_name}/error.log'
logging.basicConfig(filename=error_log_filename,
                    level=logging.ERROR,
                    format='{%(pathname)s:%(lineno)d} %(levelname)s %(funcName)s %(asctime)s %(name)s %(message)s', )

logger = logging.getLogger(__name__)
bqclient = bigquery.Client()
def get_config(filepath):
    config = open(filepath, 'r')
    content = yaml.safe_load(config)
    return content

def get_table_data(bq_table_spec):
    try:
        table = bigquery.TableReference.from_string(bq_table_spec)
        rows = bqclient.list_rows(table)
        data = rows.to_dataframe()
        return data
    except Exception as e:
        logger.error(e)


def get_bucket_data(gsutil_uri):
    try:
        if '.csv' in gsutil_uri:
            data = pd.read_csv(gsutil_uri)
        elif '.json' in gsutil_uri:
            data = pd.read_json(gsutil_uri)
        else:
            return 'filetype invalid'
        return data
    except Exception as e:
        logger.error(e)
        return False


def parse_data(source_data, target_data, report, mode):
    try:
        bool_compare_row_count = compare_row_count(source_data, target_data, report)
        bool_compare_schema = compare_schema(source_data, target_data, report, mode)
        bool_check_duplicates_source = check_duplicates(source_data, report, mode='source')
        bool_check_duplicates_target = check_duplicates(target_data, report, mode='target')
        bool_compare_diff = compare_diff(source_data, target_data, report)
        if (bool_compare_row_count and
                bool_compare_schema and
                bool_check_duplicates_source and
                bool_check_duplicates_target and
                bool_compare_diff):
            print('all matched')
            return True
        else:
            return False
    except Exception as e:
        logger.error(e)
        return False


def compare_row_count(source_data, target_data, report):
    try:
        string_to_write = '---ROW CHECK---\nsource data rows: {}\ntarget data rows:{}\n'.format(len(source_data),
                                                                                                len(target_data))
        if len(source_data) == len(target_data):
            string_to_write = string_to_write + 'remarks: PASSED\n'
            report.write(string_to_write)
            return True
        else:
            string_to_write = string_to_write + 'remarks: FAILED\n'
            report.write(string_to_write)
            return False
    except Exception as te:
        logger.error(te)
        return False


def compare_schema(source_data, target_data, report, mode):
    try:
        string_to_write = '---SCHEMA CHECK---\nmode: {}\n'.format(mode)
        
        source_schema = {}
        for i in range(len(source_data.columns)):
            source_schema[source_data.columns.tolist()[i]] = str(source_data.dtypes[i])

        target_schema = {}
        for i in range(len(target_data.columns)):
            target_schema[target_data.columns.tolist()[i]] = str(target_data.dtypes[i])

        string_to_write = string_to_write + 'source schema: {}\ntarget schema: {}\n'.format(json.dumps(source_schema),
                                                                                            json.dumps(target_schema))

        if mode == 'strict' and source_schema == target_schema:
            string_to_write = string_to_write + 'remarks: PASSED\n'
            report.write(string_to_write)
            return True
        elif mode == 'default' and source_schema.keys() == target_schema.keys():
            string_to_write = string_to_write + 'remarks: PASSED\n'
            report.write(string_to_write)
            return True
        else:
            string_to_write = string_to_write + compare_schema_diff(source_schema, target_schema) + '\nremarks: FAILED\n'
            report.write(string_to_write)
            return False
    except Exception as e:
        logger.error(e)
        return False

def compare_schema_diff(source_schema, target_schema):
    """
      Compares the difference of the source and target schema
      Args:
          source_schema (dict)
          target_schema (dict)
      Returns:
          Schema differences (dict)
    """
    merged_schema = source_schema | target_schema
    src_schema_diff, trg_schema_diff = {}, {}
    if merged_schema != source_schema:
        src_schema_diff = { k : merged_schema[k] for k in set(merged_schema) - set(target_schema) }

    if merged_schema != target_schema:
        trg_schema_diff = { k : merged_schema[k] for k in set(merged_schema) - set(source_schema) }
    
    schema_failed = f'source schema [new]: {src_schema_diff}\ntarget schema [new]: {trg_schema_diff}'
    return schema_failed

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

def check_duplicates(data, report, mode):
    try:
        duplicate_check_df = data.duplicated()
        number_of_duplicates = duplicate_check_df.sum()
        duplicate_rows = data.loc[data.duplicated(), :]
        string_to_write = '---DUPLICATE CHECK---\nmode: {}\n'.format(mode)

        if number_of_duplicates > 0:
            string_to_write = string_to_write + duplicate_rows.to_string() + '\nremarks: FAILED\n'
            report.write(string_to_write)
            return False
        else:
            string_to_write = string_to_write + 'remarks:PASSED\n'
            report.write(string_to_write)
            return True
    except Exception as e:
        logger.error(e)
        return False


# removes empty error log
logging.shutdown()
if os.stat(error_log_filename).st_size == 0:
    os.remove(error_log_filename)


def get_sample_values(data):
    return data.head(10)
