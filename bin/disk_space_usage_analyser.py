import argparse
import logging
import os
import sys

from egcg_core.app_logging import AppLogger, logging_default as log_cfg
from egcg_core.config import cfg


class DiskSpaceUsageAnalysis(AppLogger):
    # This command is used throughout to retrieve the disk usage of each directory checked
    space_command = "du -ck "
    dir_cfg = ""

    def __init__(self):
        # Loading the run and project directory values from the config file
        DiskSpaceUsageAnalysis.dir_cfg = cfg.query('directory_space_analysis', ret_default={})
        if set(DiskSpaceUsageAnalysis.dir_cfg) != {'runs_dir', 'projects_dir', 'output_dir'}:
            self.error('Directory config invalid or incomplete. Please ensure your config has an entry for '
                       'directory_space_analysis.')
            return


def main():
    arg_parser = argparse.ArgumentParser()
    arg_group = arg_parser.add_mutually_exclusive_group(required=True)
    arg_group.add_argument('all', help='Runs all available checks - '
                                       'Run directory check, residual run and project directory checks.', nargs='?')
    arg_group.add_argument('runs_directory', help='Check the runs directory for space used storing samples, '
                                                  'and displays the respective archiving status.', nargs='?')
    arg_group.add_argument('residual_runs_directory', help='Check residual space used when the samples directories` space used '
                                                           'is deducted from the respective runs directory`s space used.', nargs='?')
    arg_group.add_argument('residual_projects_directory', help='Check the projects directory for space used storing samples, '
                                                               'and displays the respective archiving status. Also checks the '
                                                               'residual space used when the samples directories` space used '
                                                               'is deducted from the respective runs directory`s space used.',
                           nargs='?')

    args = arg_parser.parse_args()

    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config import load_config
    from disk_space_usage_analysis import RunDirectoryChecker

    load_config()
    log_cfg.set_log_level(logging.INFO)
    log_cfg.add_stdout_handler()

    # Interpret parameter and select appropriate function
    if args.all:
        pass
    elif args.runs_directory:
        run_directory_checker = RunDirectoryChecker()
        run_directory_checker.execute()

    elif args.residual_runs_directory:
        pass
    elif args.residual_projects_directory:
        pass


if __name__ == '__main__':
    main()
