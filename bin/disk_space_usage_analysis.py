import argparse
import csv
import logging
import os
import sys
from collections import Counter
from config import load_config

from egcg_core import rest_communication
from egcg_core.app_logging import AppLogger, logging_default as log_cfg
from egcg_core.config import cfg
from egcg_core.exceptions import EGCGError


class DiskSpaceUsageAnalysis(AppLogger):
    def __init__(self):
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

        # This command is used throughout to retrieve the disk usage of each directory checked
        self.space_command = "du -ck "

        # Loading the run and project directory values from the config file
        self.dir_cfg = cfg.query('runs_dir_space_analysis', 'projects_dir_space_analysis', ret_default={})
        if set(self.dir_cfg) != {'runs_dir', 'projects_dir'}:
            self.error('Directory config invalid or incomplete. Please ensure your config has an entry for '
                       'runs_directory and projects_directory.')
            return

class RunDirectoryChecker(AppLogger):
    def __init__(self):
        self.directory_set = set()
        self.sample_counter = Counter()
        self.sample_splits = Counter()
        self.deleted_dict = {}

        self.bash_command = "find " + DiskSpaceUsageAnalysis.dir_cfg['runs_dir_space_analysis']['runs_dir'] \
                            + ". -name '*.fastq.gz' -type f | egrep -v '/fastq/fastq'"


    def run_directory_check():


# samples = c.get_documents('samples', projection={"data_deleted":1} ,max_results=1000, all_pages=True)

# cfg['run']['output_dir']    # Run directories
# cfg['sample']['output_dir'] # project directories


def main():
    arg_parser = argparse.ArgumentParser()
    arg_group = arg_parser.add_mutually_exclusive_group(required=True)
    arg_group.add_argument('all', help='Runs all available checks - '
                               'Run directory check, residual run and project directory checks.')
    arg_group.add_argument('runs_directory', help='Check the runs directory for space used storing samples, '
                                         'and displays the respective archiving status.')
    arg_group.add_argument('residual_runs_directory', help='Check residual space used when samples directories space used '
                                                          'is deducted from the respective runs directory space used.')
    arg_group.add_argument('residual_projects_directory', help='Check residual space used when samples ')

    args = arg_parser.parse_args()
    load_config()
    log_cfg.set_log_level(logging.INFO)
    log_cfg.add_stdout_handler()

    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)


if __name__ == '__main__':
    main()