import sys
import datetime
from common import get_config
from gcs_to_bq import gcs_to_bq
from gcs_to_gcs import gcs_to_gcs
from bq_to_bq import bq_to_bq

if __name__ in '__main__':
    current_time = datetime.datetime.now().strftime('%m-%d-%Y_%H.%M.%S')
    cli_arguments = sys.argv
    config_filepath = cli_arguments[1]
    config = get_config(config_filepath)['job_configs']
    try:
        report = open('anomaly_report_{}.txt'.format(current_time), 'x')
    except FileExistsError:
        report = open('anomaly_report_{}.txt'.format(current_time), 'w')

    report.write('Anomaly report {}\n'.format(current_time))
    
    for job in config:
        module_to_run = job['job_parameters'][0]['name']
        sources = job['job_parameters'][1]['sources']
        targets = job['job_parameters'][2]['targets']
        mode = job['job_parameters'][3]['mode']

        if module_to_run == 'gcs_to_gcs':
            print('running gcs to gcs')
            gcs_to_gcs(sources, targets, mode, report)
        elif module_to_run == 'gcs_to_bq':
            print('running gcs to bq')
            gcs_to_bq(sources, targets, mode, report)
        elif module_to_run == 'bq_to_bq':
            print('running bq to bq')
            bq_to_bq(sources, targets, mode, report)
    report.close()



    