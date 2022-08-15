from common import get_bucket_data
from common import get_table_data
from common import parse_data

def gcs_to_bq(sources, targets, mode, report):
    report.write('module: gcs_to_bq\n')

    for i in range(len(sources)):
        source_data = get_bucket_data(sources[i])
        target_data = get_table_data(targets[i])
        report.write('source: {}\ntarget: {}\n'.format(sources[i], targets[i]))
        results = parse_data(source_data, target_data, report, mode=mode)
