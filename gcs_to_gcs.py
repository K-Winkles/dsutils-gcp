import sys
from common import get_bucket_data
from common import parse_data

if __name__ in '__main__':
    cli_arguments = sys.argv
    gsutil_uri_source = cli_arguments[1]
    gsutil_uri_target = cli_arguments[2]

    source_data = get_bucket_data(gsutil_uri_source)
    target_data = get_bucket_data(gsutil_uri_target)
    results = parse_data(source_data, target_data, mode='default')
