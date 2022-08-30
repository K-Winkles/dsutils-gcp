import sys
from logger import *
from common import get_config
#from gcs_to_bq import gcs_to_bq
from gcs_to_gcs import gcs_to_gcs
#from bq_to_bq import bq_to_bq
from common import logger as logging # import logger config from commons.py

if __name__ in '__main__':
    cli_arguments = sys.argv
    config_filepath = cli_arguments[1]
    config = get_config(config_filepath)['job_configs']   
    
    yaml_dump(config_filepath)                          # overwrites global variables in yaml
    create_anomaly_error(config_filepath)               # creates empty anomaly report and error log
    print("Anomaly report and error log created...")

    try:
        for job in config:
            module_to_run = job['job_parameters'][0]['name']
            sources = job['job_parameters'][1]['sources']
            targets = job['job_parameters'][2]['targets']
            mode = job['job_parameters'][3]['mode']

            if module_to_run == 'gcs_to_gcs':
                print('running gcs to gcs')
                gcs_to_gcs(sources, targets, mode)
            #elif module_to_run == 'gcs_to_bq':
            #    print('running gcs to bq')
            #    gcs_to_bq(sources, targets, mode)
            #elif module_to_run == 'bq_to_bq':
            #    print('running bq to bq')
            #    bq_to_bq(sources, targets, mode)
    except Exception as e:
        logging.error(e)