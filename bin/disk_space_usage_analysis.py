import argparse
import csv
import logging
import os
import sys
from collections import Counter
from config import load_config
from egcg_core.app_logging import logging_default

from egcg_core.config import cfg
from egcg_core.rest_communication import Communicator

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# This command is used throughout to retrieve the disk usage of each directory checked
space_command = "du -ck "

# This communicator will be used throughout to download data from the Reporting App Rest API
c = Communicator((username, password), 'https://egreports.mvm.ed.ac.uk/api/0.1/')


class RunDirectoryChecker:
    def __init__(self):
        self.directory_set = set()
        self.sample_counter = Counter()
        self.sample_splits = Counter()
        self.deleted_dict = {}

        self.bash_command = "find /lustre/edgeprod/processed/runs/*/*/*/*.fastq.gz -type f | egrep -v '/fastq/fastq'"


def run_directory_check():


# samples = c.get_documents('samples', projection={"data_deleted":1} ,max_results=1000, all_pages=True)

# cfg['run']['output_dir']    # Run directories
# cfg['sample']['output_dir'] # project directories


def main():
    a = argparse.ArgumentParser()
    a.add_argument('all', help='Runs all available checks - '
                               'Run directory check, residual run and project directory checks.')
    a.add_argument('run_directory', help='Check the run directory for space used storing samples, '
                                         'and displays the respective archiving status.')
    a.add_argument('residual_run_directory', help='Check space used when sample directories space used ')

    args = a.parse_args()
    load_config()

    logging_default.add_stdout_handler()
    if args.debug:
        logging_default.set_log_level(logging.DEBUG)


if __name__ == '__main__':
    main()