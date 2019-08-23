import argparse
import logging
import os
import sys

from egcg_core.app_logging import AppLogger, logging_default as log_cfg
from egcg_core.config import cfg


class DiskSpaceUsageAnalysisHelper(AppLogger):
    # This command is used throughout to retrieve the disk usage of each directory checked
    space_command = "du -ck "

    def __init__(self):
        # Loading the run and project directory values from the config file
        self.dir_cfg = cfg.query('directory_space_analysis', ret_default={})
        if set(self.dir_cfg) != {'runs_dir', 'projects_dir', 'output_dir'}:
            self.error('Directory config invalid or incomplete. Please ensure your config has an entry for '
                       'directory_space_analysis: runs_dir, projects_dir, and output_dir')


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-dir', help='"all", "run" or "project" - '
                            'All directory checks, run directory check, or project directory checks. All '
                                         'is run by default.',
                            action="store", default="all", nargs='?')
    arg_parser.add_argument('-debug', help='Set the logging level to Debug.', action="store", default="False",
                            nargs='?')

    args = arg_parser.parse_args()

    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config import load_config
    from disk_space_usage_analysis import RunDirectoryChecker

    load_config()

    log_cfg.set_log_level(logging.INFO)
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)
    log_cfg.add_stdout_handler()

    # Load Disk Space Usage Analysis config by creating new object
    disk_usage_helper = DiskSpaceUsageAnalysisHelper()

    # Interpret parameter and select appropriate function
    if args.dir == 'all':
        pass
    elif args.dir == 'run':
        run_directory_checker = RunDirectoryChecker(disk_usage_helper, log_cfg)
        run_directory_checker.execute()
    elif args.dir == 'project':
        pass


if __name__ == '__main__':
    main()
