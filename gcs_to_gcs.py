from common import get_bucket_data
from common import parse_data
from logger import *


def gcs_to_gcs(sources, targets, mode):
    anomaly_report('\nmodule: gcs_to_gcs\n')

    for i in range(len(sources)):
        source_data = get_bucket_data(sources[i])
        target_data = get_bucket_data(targets[i])
        anomaly_report('source: {}\ntarget: {}\n'.format(sources[i], targets[i]))
        results = parse_data(source_data, target_data, mode=mode)