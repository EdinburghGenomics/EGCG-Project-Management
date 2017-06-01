import argparse
import csv
import glob
import logging
import os
import sys
from collections import defaultdict
from egcg_core.app_logging import AppLogger, logging_default as log_cfg
from egcg_core.config import cfg
from os.path import dirname, basename, join

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config

file_extensions_to_check = [
    'fastq.gz',
    'bam',
    'g.vcf.gz'
]

def parse_aspera_reports(report_csv):
    all_files = defaultdict(list)
    with open(report_csv) as open_report:
        # ignore what is before the second blank lines
        blank_line = 0
        for l in open_report:
            if not l.strip():
                blank_line += 1
            if blank_line == 2:
                break

        dict_reader = csv.DictReader(open_report)
        for file_dict in dict_reader:
            f = '/'.join(file_dict['file_path'].split('/')[3:])
            all_files[f].append(file_dict)
    return all_files


def merge_file_lists(file_lists):
    all_files = defaultdict(list)
    for file_list in file_lists:
        for f in file_list:
            all_files[f].extend(file_list[f])
    return all_files


def check_end_file_name(f):
    for ext in file_extensions_to_check:
        if f.endswith(ext):
            return True
    return False


def list_files_delivered(path):
    all_files = {}
    for root, dirs, files in os.walk(path):
        relative_root = root.replace(path, '')[1:]
        for f in files:
            if check_end_file_name(f):
                all_files[os.path.join(relative_root, f)] = os.stat(os.path.join(root, f))
    return all_files


class ConfirmDelivery(AppLogger):
    def __init__(self, aspera_report_csv_files):
        self.confirmed_files = merge_file_lists([parse_aspera_reports(f) for f in aspera_report_csv_files])
        self.delivery_dir = cfg.query('delivery_dest')


    def test_sample(self, sample_id):
        sample_folders = glob.glob(os.path.join(self.delivery_dir, '*', '*', sample_id))
        if not sample_folders:
            raise ValueError('%s Has not been delivered yet' % (sample_id))
        elif len(sample_folders) > 1:
            self.warning('More than one delivery for sample %s', sample_id)
        for sample_folder in sample_folders:
            staging_date = basename(dirname(sample_folder))
            project_id = basename(dirname(dirname(sample_folder)))
            if self._test_sample(project_id, staging_date, sample_id):
                self.info('Confirmed %s:%s -- %s', project_id, staging_date, sample_id)
            else:
                self.info('Not confirmed %s:%s -- %s', project_id, staging_date, sample_id)


    def _test_sample(self, project_id, staging_date, sample_id):
        sample_folder = os.path.join(self.delivery_dir, project_id, staging_date, sample_id)
        files_to_test = list_files_delivered(sample_folder)
        files_to_test = [join(project_id, staging_date, sample_id, f) for f in files_to_test]
        files_found, files_not_found = self._compare_files_lists(files_to_test)
        if files_not_found:
            for f in files_not_found:
                self.debug('Not found %s', f)
            return False
        else:
            for f in files_found:
                self.debug('found %s', f)
            return True

    def _compare_files_lists(self, files_to_test):
        files_found = []
        files_not_found = []
        for f in files_to_test:
            if f in self.confirmed_files:
                files_found.append(f)
            else:
                files_not_found.append(f)
        return (files_found, files_not_found)

    def compare_all_files(self):
        files_to_test = list_files_delivered(self.delivery_dir)
        for f in files_to_test:
            if f in self.confirmed_files:
                self.info('found %s', f)
            else:
                self.info('Not found %s', f)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--samples', type=str, nargs='+')
    p.add_argument('--csv_files', type=str, required=True, nargs='+')
    p.add_argument('--debug', action='store_true')
    args = p.parse_args()

    load_config()

    log_cfg.add_handler(logging.StreamHandler(stream=sys.stdout), level=logging.INFO)
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    cfg.merge(cfg['sample'])

    cd = ConfirmDelivery(args.csv_files)
    if args.samples:
        for sample in args.samples:
            cd.test_sample(sample)
    else:
        cd.compare_all_files()


if __name__ == '__main__':
    main()
