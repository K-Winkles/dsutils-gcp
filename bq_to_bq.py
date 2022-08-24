from common import get_table_data
from common import parse_data
from logger import *

def bq_to_bq(sources, targets, mode):
    anomaly_report('\nmodule: bq_to_bq\n')

    for i in range(len(sources)):
        source_data = get_table_data(sources[i])
        target_data = get_table_data(targets[i])
        anomaly_report('source: {}\ntarget: {}\n'.format(sources[i], targets[i]))
        results = parse_data(source_data, target_data, mode=mode)
