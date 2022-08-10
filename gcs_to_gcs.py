import sys
import datetime
from common import get_bucket_data
from common import parse_data

current_time = datetime.datetime.now().strftime('%m-%d-%Y_%H.%M.%S')
report = open('anomaly_report_{}.txt'.format(current_time), 'x')

if __name__ in '__main__':
    cli_arguments = sys.argv
    gsutil_uri_source = cli_arguments[1]
    gsutil_uri_target = cli_arguments[2]

    report.write('Anomaly report {}\nsource data: {}\ntarget data: {}'.format(
        current_time, 
        gsutil_uri_source, 
        gsutil_uri_target
    ))

    source_data = get_bucket_data(gsutil_uri_source)
    target_data = get_bucket_data(gsutil_uri_target)
    results = parse_data(source_data, target_data, report, mode='default')
    report.close()
