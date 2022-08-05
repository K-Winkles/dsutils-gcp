import sys
from common import get_bucket_data
from common import get_table_data
from common import parse_data

if __name__ in '__main__':
    cli_arguments = sys.argv
    gsutil_uri = cli_arguments[1]
    bq_table_spec = cli_arguments[2]

    source_data = get_bucket_data(gsutil_uri)
    target_data = get_table_data(bq_table_spec)
    results = parse_data(source_data, target_data, mode='default')
