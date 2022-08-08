import sys
from common import get_table_data
from common import parse_data

if __name__ in '__main__':
    cli_arguments = sys.argv
    bq_table_spec_source = cli_arguments[1]
    bq_table_spec_target = cli_arguments[2]

    source_data = get_table_data(bq_table_spec_source)
    target_data = get_table_data(bq_table_spec_target)
    results = parse_data(source_data, target_data, mode='default')
