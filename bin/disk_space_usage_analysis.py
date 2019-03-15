import argparse
import csv
import logging
import os
import sys
from collections import Counter

from egcg_core import rest_communication
from egcg_core.app_logging import AppLogger, logging_default as log_cfg
from egcg_core.config import cfg
from egcg_core.exceptions import EGCGError


class DiskSpaceUsageAnalysis(AppLogger):
    def __init__(self):
        # This command is used throughout to retrieve the disk usage of each directory checked
        self.space_command = "du -ck "

        # Loading the run and project directory values from the config file
        self.dir_cfg = cfg.query('runs_dir_space_analysis', 'projects_dir_space_analysis', ret_default={})
        print(cfg.content)
        print(self.dir_cfg)
        print(set(self.dir_cfg))
        if set(self.dir_cfg) != {'runs_dir', 'projects_dir'}:
            self.error('Directory config invalid or incomplete. Please ensure your config has an entry for '
                       'runs_dir_space_analysis and projects_dir_space_analysis.')
            return


class RunDirectoryChecker(AppLogger):
    def __init__(self, disk_space_usage_analysis):
        self.disk_space_usage_analysis = disk_space_usage_analysis
        self.directory_set = set()
        self.sample_counter = Counter()
        self.sample_splits = Counter()
        self.deleted_dict = {}

        self.bash_command = "find " + disk_space_usage_analysis.dir_cfg['runs_dir_space_analysis']['runs_dir'] \
                            + ". -name '*.fastq.gz' -type f | egrep -v '/fastq/fastq'"

    # Aggregates directory space used and checks whether the document has been archived
    def run_directory_check(self):
        output = os.popen(self.bash_command).read()

        for sample_directory_path in output.splitlines():
            sample_directory_path_split = sample_directory_path.split('/')
            if len(sample_directory_path_split) == 9:
                try:
                    sample_name = sample_directory_path_split[7]
                except IndexError:
                    self.error('Index Error when splitting sample directory path.')
                    continue

                directory_path = '/{lustre}/{env}/{proc}/{runs}/{directory}/{project}/{sample}'.format(
                    lustre=sample_directory_path_split[1], env=sample_directory_path_split[2],
                    proc=sample_directory_path_split[3], runs=sample_directory_path_split[4],
                    directory=sample_directory_path_split[5], project=sample_directory_path_split[6],
                    sample=sample_directory_path_split[7])
                if directory_path not in self.directory_set:
                    self.directory_set.add(directory_path)
                    command = self.disk_space_usage_analysis.space_command + directory_path
                    command_output = os.popen(command).read()
                    self.sample_splits.update({sample_name: 1})
                    self.sample_counter.update({sample_name: int(command_output.split()[0])})
                    try:
                        data_deleted = rest_communication.get_document(
                            'samples', where={'sample_id': sample_name})['data_deleted']
                    except TypeError:
                        data_deleted = "Error - not found"
                    self.deleted_dict[sample_name] = data_deleted

    # Exports the run directory analysis to a CSV file
    def export_run_directory_analysis_csv(self):
        with open(self.disk_space_usage_analysis.dir_cfg['runs_dir_space_analysis']['output_dir'] + 'run_dir_analysis.csv',
                  mode='w') as analysis_file:
            file_writer = csv.writer(analysis_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            file_writer.writerow(['Sample Name', 'Size', 'Data Deleted', 'Splits'])

            for key, value in self.sample_counter.most_common():
                data_deleted = self.deleted_dict[key]
                num_splits = self.sample_splits[key]
                file_writer.writerow([key, str(value), data_deleted, str(num_splits)])

    # Exports the run directory analysis to a TXT file
    def export_run_directory_analysis_txt(self):
        with open(self.disk_space_usage_analysis.dir_cfg['runs_dir_space_analysis']['output_dir'] + 'run_dir_analysis.txt',
                  mode='w') as analysis_txt_file:
            analysis_txt_file.write('Samples and space taken as follows: ')

            for key, value in self.sample_counter.most_common():
                analysis_txt_file.write(key + ': ' + str(value))
                analysis_txt_file.write('Data Deleted: ' + self.deleted_dict[key])

            analysis_txt_file.write('Samples were split as follows: ')
            for key, value in self.sample_splits.most_common():
                analysis_txt_file.write(key + ': ' + str(value))

    def execute(self):
        self.run_directory_check()
        self.export_run_directory_analysis_csv()
        self.export_run_directory_analysis_txt()

# samples = c.get_documents('samples', projection={"data_deleted":1} ,max_results=1000, all_pages=True)


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

    load_config()
    log_cfg.set_log_level(logging.INFO)
    log_cfg.add_stdout_handler()

    disk_space_usage_analysis = DiskSpaceUsageAnalysis()

    # Interpret parameter and select appropriate function
    if args.all:
        pass
    elif args.runs_directory:
        run_directory_checker = RunDirectoryChecker(disk_space_usage_analysis)
        run_directory_checker.execute()

    elif args.residual_runs_directory:
        pass
    elif args.residual_projects_directory:
        pass


if __name__ == '__main__':
    main()
