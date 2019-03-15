import csv
import os
from collections import Counter

from egcg_core import rest_communication
from egcg_core.app_logging import AppLogger


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
