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


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('dirs', help='"all", "run" or "project" - '
                            'All directory checks, run directory check, or project directory checks.', nargs='?',
                            required=True)

    args = arg_parser.parse_args()

    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config import load_config
    from disk_space_usage_analysis import RunDirectoryChecker

    load_config()
    log_cfg.set_log_level(logging.INFO)
    log_cfg.add_stdout_handler()

    # Interpret parameter and select appropriate function
    if args.dirs == 'all':
        pass
    elif args.dirs == 'run':
        run_directory_checker = RunDirectoryChecker()
        run_directory_checker.execute()
    elif args.dirs == 'project':
        pass


if __name__ == '__main__':
    main()
