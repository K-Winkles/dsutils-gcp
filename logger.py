import os, pathlib
import datetime
import yaml
import logging

def get_global_config(filepath):
    """Opens and reads config yaml"""
    with open(filepath, 'r') as config:
        return yaml.safe_load(config)


def yaml_dump(filepath):
    """Generates global variables and dump to config.yml file"""
    datetime_now = datetime.datetime.now()
    current_date = datetime_now.strftime('%m-%d-%Y')
    current_time = datetime_now.strftime('%H.%M.%S')
    anomaly_report_filename = f'anomaly_report_{current_date}_{current_time}.txt'
    error_filename = 'error.log'
    current_folder = os.path.join(os.getcwd(), 'logs', f'{current_date}\{current_time}')

    global_vars = get_global_config(filepath)
    global_vars['global_variables'][0]['current_time'] = current_time
    global_vars['global_variables'][1]['current_date'] = current_date
    global_vars['global_variables'][2]['anomaly_report_filename'] = anomaly_report_filename
    global_vars['global_variables'][3]['error_filename'] = error_filename
    global_vars['global_variables'][4]['current_folder'] = current_folder

    with open(filepath, 'w') as config:
        yaml.dump(global_vars, config)


def create_anomaly_error(filepath):
    """Creates empty anomaly report and error log"""
    global_vars = get_global_config(filepath)['global_variables']
    current_time = global_vars[0]['current_time']
    current_date = global_vars[1]['current_date']
    anomaly_report_filename = global_vars[2]['anomaly_report_filename']
    error_filename = global_vars[3]['error_filename']
    current_folder = global_vars[4]['current_folder']
    
    pathlib.Path(f'{current_folder}').mkdir(parents=True, exist_ok=True)         # creates anomaly and log folder

    anomaly_filepath = os.path.join(current_folder, anomaly_report_filename)
    report_write(anomaly_filepath, f'Anomaly report {current_date} {current_time}\n')

    error_filepath = os.path.join(current_folder, error_filename)
    report_write(error_filepath, 'Error logs\n')

    
def report_write(filepath, string_to_write):
    """Write or append strings to write on the specified file"""
    mode = 'a+' if os.path.exists(filepath) else 'w'
    with open(filepath, mode) as report:
        report.write(string_to_write)


def anomaly_report(string_to_write):
    """Write or logs anomaly reports"""
    global_vars = get_global_config('config.yml')['global_variables']
    anomaly_report_filename = global_vars[2]['anomaly_report_filename']
    current_folder = global_vars[4]['current_folder']

    report_write(os.path.join(current_folder, anomaly_report_filename), 
                 string_to_write)


def error_logging():
    """Error logging"""
    global_vars = get_global_config('config.yml')['global_variables']
    error_filename = global_vars[3]['error_filename']
    current_folder = global_vars[4]['current_folder']

    logging.basicConfig(filename=os.path.join(current_folder, error_filename),
                        level=logging.ERROR,
                        format='{%(pathname)s:%(lineno)d} %(levelname)s %(funcName)s %(asctime)s %(name)s %(message)s', )
    logger = logging.getLogger(__name__)

    return logger
