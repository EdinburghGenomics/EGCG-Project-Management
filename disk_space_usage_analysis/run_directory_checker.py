import csv
import os
from collections import Counter

from egcg_core import rest_communication
from egcg_core.app_logging import AppLogger


class RunDirectoryChecker(AppLogger):
    # Initialising instance variables
    def __init__(self, helper):
        self.disk_usage_helper = helper
        self.deleted_dict = {}
        self.directory_set = set()
        self.sample_counter = Counter()
        self.sample_splits = Counter()
        self.run_counter = Counter()
        self.run_sample_counter = Counter()
        self.output_dir = helper.dir_cfg['output_dir']

        self.bash_command = "find " + helper.dir_cfg['runs_dir'] + ". -name '*.fastq.gz' -type f | egrep -v '/fastq/fastq'"

    # Aggregates directory space used and checks whether the document has been archived
    def _run_directory_check(self):
        output = os.popen(self.bash_command).read()

        for sample_directory_path in output.splitlines():
            sample_directory_path_split = sample_directory_path.split('/')
            try:
                assert len(sample_directory_path_split) == 9

                # Generating named variables from directory path split
                run_directory_name = sample_directory_path_split[5]
                sample_name = sample_directory_path_split[7]

                # Generating directory paths
                run_directory_path = '/'.join(sample_directory_path_split[:6])
                directory_path = '/'.join(sample_directory_path_split[:8])

                if directory_path not in self.directory_set:
                    # Adding it to directory set
                    self.directory_set.add(directory_path)
                    command = self.disk_usage_helper.space_command + directory_path
                    command_output = os.popen(command).read()

                    # Adding/Incrementing value in sample_splits, run_sample_counter and sample_counter
                    self.sample_splits.update({sample_name: 1})
                    self.run_sample_counter.update({run_directory_name: int(command_output.split()[0])})
                    self.sample_counter.update({sample_name: int(command_output.split()[0])})

                    try:
                        data_deleted = rest_communication.get_document('samples',
                                                                       where={'sample_id': sample_name})['data_deleted']
                    except TypeError:
                        data_deleted = "Error - not found"
                    self.deleted_dict[sample_name] = data_deleted

                if run_directory_name not in self.run_counter:
                    # Not in run directory set - adding it
                    command = self.disk_usage_helper.space_command + run_directory_path
                    command_output = os.popen(command).read()
                    self.run_counter.update({run_directory_name: int(command_output.split()[0])})
            except AssertionError:
                self.error('Index Error when splitting sample directory path: ' + sample_directory_path)
                continue

    # Calculates and exports the run directory analysis to a CSV file
    def _export_run_directory_analysis_csv(self):
        with open(self.output_dir + 'run_dir_analysis.csv', mode='w+') as file:
            file_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            file_writer.writerow(['Sample Name', 'Size', 'Data Deleted', 'Splits'])

            for key, value in self.sample_counter.most_common():
                data_deleted = self.deleted_dict[key]
                num_splits = self.sample_splits[key]
                file_writer.writerow([key, str(value), data_deleted, str(num_splits)])

    # Exports the run directory analysis to a TXT file
    def _export_run_directory_analysis_txt(self):
        with open(self.output_dir + 'run_dir_analysis_log.txt', mode='w+') as analysis_txt_file:
            analysis_txt_file.write('Samples and space taken as follows: ')

            for key, value in self.sample_counter.most_common():
                analysis_txt_file.write(key + ': ' + str(value))
                analysis_txt_file.write('Data Deleted: ' + self.deleted_dict[key])

            analysis_txt_file.write('Samples were split as follows: ')
            for key, value in self.sample_splits.most_common():
                analysis_txt_file.write(key + ': ' + str(value))

    # Calculates and exports the residual run directory analysis to a CSV file
    def _export_residual_run_directory_analysis(self):
        with open(self.output_dir + 'residual_run_directory_analysis.csv', mode='w+') as file:
            file_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            file_writer.writerow(['Run Directory Name', 'Residual Size'])

            for key, value in self.run_counter.most_common():
                residual_space = value - self.run_sample_counter[key]
                file_writer.writerow([key, str(residual_space)])

    # Exports the residual run directory analysis to a TXT file
    def _export_residual_run_directory_analysis_txt(self):
        with open(self.output_dir + 'residual_run_dir_analysis_log.txt', mode='w+') as analysis_txt_file:
            analysis_txt_file.write('Run directories space taken as follows: ')
            for key, value in self.run_counter.most_common():
                analysis_txt_file.write(key + ': ' + str(value))

            analysis_txt_file.write('Sample run directories took space as follows: ')
            for key, value in self.run_sample_counter.most_common():
                analysis_txt_file.write(key + ': ' + str(value))

    # Main function which executes all
    def execute(self):
        self._run_directory_check()
        self._export_run_directory_analysis_csv()
        self._export_run_directory_analysis_txt()
        self._export_residual_run_directory_analysis()
        self._export_residual_run_directory_analysis_txt()

# samples = c.get_documents('samples', projection={"data_deleted":1} ,max_results=1000, all_pages=True)
