import datetime

from common import get_bucket_data
from common import get_table_data
from common import parse_data

datetime_now = datetime.datetime.now()
current_time = datetime_now.strftime('%m-%d-%Y_%H.%M.%S')
current_folder_name = f'{datetime_now.strftime("%m-%d-%Y")}/{datetime_now.strftime("%H.%M.%S")}'
report = open(f'logs/{current_folder_name}/anomaly_report_{current_time}.txt', 'x')

def gcs_to_bq(sources, targets, mode, report):
    report.write('module: gcs_to_bq\n')

    for i in range(len(sources)):
        source_data = get_bucket_data(sources[i])
        target_data = get_table_data(targets[i])
        report.write('source: {}\ntarget: {}\n'.format(sources[i], targets[i]))
        results = parse_data(source_data, target_data, report, mode=mode)
