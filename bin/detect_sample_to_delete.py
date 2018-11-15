import os
import sys
import csv
import argparse
import logging
import operator
from collections import defaultdict
from datetime import datetime, timedelta
from egcg_core import rest_communication, clarity
from egcg_core.app_logging import logging_default as log_cfg, AppLogger
from egcg_core.notifications import send_plain_text_email
from egcg_core.config import cfg

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config

data_release_step_names = {'Data Release EG 1.0', 'Data Release EG 2.0 ST'}


def _utcnow():
    return datetime.utcnow()


class SampleToDeleteDetector(AppLogger):

    def __init__(self):
        self._cache_sample_to_release_date = {}
        self._cache_sample_to_lims_statuses = {}

    @staticmethod
    def _download_confirmation(sample_data):
        # TODO: need to check the LIMS for download confirmation when implemented there
        # for now look only at the files downloaded
        files_downloaded = set([f['file_path'] for f in sample_data.get('files_downloaded', [])])
        files_delivered = sample_data.get('files_delivered', [])
        files_missing = [f['file_path'] for f in files_delivered if f['file_path'] not in files_downloaded]
        return not bool(files_missing)

    @staticmethod
    def _get_release_date_from_sample_statuses(statuses):
        for status in reversed(statuses):
            for process in reversed(status.get('processes')):
                if process.get('name') in data_release_step_names:
                    return datetime.strptime(process.get('date'), '%b %d %Y')

    def _get_status_from_sample(self, project_id, sample_id):
        if sample_id not in self._cache_sample_to_lims_statuses:
            self.debug('Query LIMS status for project %s', project_id)
            # It is much faster to query per project than querying each sample individually.
            lims_statuses = rest_communication.get_documents('lims/sample_status', match={"project_id": project_id, 'project_status': 'all'}, quiet=True)
            for sample in lims_statuses:
                self._cache_sample_to_lims_statuses[sample.get('sample_id')] = sample.get('statuses')
        return self._cache_sample_to_lims_statuses.get(sample_id)

    def _get_release_date(self, project_id, sample_id):
        if sample_id not in self._cache_sample_to_release_date:
            statuses = self._get_status_from_sample(project_id, sample_id)
            release_date = self._get_release_date_from_sample_statuses(statuses)
            if not release_date:
                self.debug('Query LIMS API for sample %s', sample_id)
                rdate = clarity.get_sample_release_date(sample_id)
                if rdate:
                    release_date = datetime.strptime(rdate, '%Y-%m-%d')
            self._cache_sample_to_release_date[sample_id] = release_date
        return self._cache_sample_to_release_date.get(sample_id)

    def check_samples_final_deletion(self, age_threshold=365):
        date_threshold = _utcnow() - timedelta(days=age_threshold)

        sample_records = rest_communication.get_documents(
            'samples',
            quiet=True,
            where={'useable': 'yes', 'delivered': 'yes', 'data_deleted': 'on lustre'},
            all_pages=True,
            max_results=100
        )
        projects = defaultdict(list)
        projects_to_release_dates = defaultdict(set)
        self.info('Found %s samples to check', len(sample_records))
        self.info('Found %s projects to check', len(set(s.get('project_id') for s in sample_records)))

        for r in sample_records:
            release_date = self._get_release_date(r.get('project_id'), r.get('sample_id'))

            if release_date and release_date < date_threshold:
                projects[r.get('project_id')].append(r.get('sample_id'))
                projects_to_release_dates[r.get('project_id')].add(release_date)
        for project_id, release_dates in sorted(
                projects_to_release_dates.items(),
                reverse=True,
                key=lambda kv: sorted(kv[1], reverse=True)[0]):
            for sample_id in projects[project_id]:
                self.info('%s\t%s\t%s', project_id, sorted(release_dates, reverse=True)[0].strftime('%Y-%m-%d'), sample_id)

    def check_deletable_samples(self, age_threshold=None):
        sample_records = rest_communication.get_documents(
            'samples',
            quiet=True,
            where={'useable': 'yes', 'delivered': 'yes', 'data_deleted': 'none'},
            all_pages=True,
            max_results=100
        )
        if age_threshold is None:
            age_threshold = cfg.query('data_deletion', 'age_threshold', ret_default=90)
        date_threshold = datetime.now() - timedelta(days=age_threshold)

        project_batches = defaultdict(list)
        for r in sample_records:
            release_date = self._get_release_date(r.get('project_id'), r.get('sample_id'))
            confirmation = self._download_confirmation(r)
            if release_date and release_date < date_threshold:
                pb = (r.get('project_id'), release_date)
                project_batches[pb].append((r.get('sample_id'), confirmation))

        today = _utcnow().strftime('%Y-%m-%d')
        output_dir = cfg.query('data_deletion', 'log_dir')
        if not output_dir or not os.path.exists(output_dir):
            output_dir = os.getcwd()
        output_file = os.path.join(output_dir, 'Candidate_samples_for_deletion_gt_%s_days_old_%s.csv' % (age_threshold, today))
        self.write_report(project_batches, output_file)
        msg = '''Hi,
The attached csv file contains all samples ready for deletion on the {today}.
Please review them and get back to the bioinformatics team with samples that can be deleted.
'''.format(today=today)
        send_plain_text_email(
            msg=msg,
            subject='Samples ready for deletion',
            attachments=[output_file],
            **cfg['data_deletion']['email_notification']
        )

    @staticmethod
    def write_report(project_batches, output_file):
        # format report
        headers = ['Project id', 'Release date', 'Nb sample confirmed', 'Nb sample not confirmed',
                   'Download not confirmed', 'Download confirmed']
        with open(output_file, 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow(headers)

            # sort by release date
            batch_keys = sorted(project_batches, key=operator.itemgetter(1))
            for pb in batch_keys:
                project_id, release_date = pb
                out = [project_id, release_date.strftime('%Y-%m-%d')]
                list_sample = project_batches.get(pb)
                sample_confirmed = [sample for sample, confirmed in list_sample if confirmed]
                sample_not_confirmed = [sample for sample, confirmed in list_sample if not confirmed]
                out.append(str(len(sample_confirmed)))
                out.append(str(len(sample_not_confirmed)))
                out.append(' '.join(sorted(sample_not_confirmed)))
                out.append(' '.join(sorted(sample_confirmed)))
                writer.writerow(out)


def main():
    args = _parse_args()
    detector = SampleToDeleteDetector()

    load_config()
    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)
    if args.final_deletion:
        if args.age_threshold is None:
            logging.info('Use default age threshold of 365 days')
            age_threshold = 365
        else:
            age_threshold = args.age_threshold
        detector.check_samples_final_deletion(age_threshold)
    else:
        if args.age_threshold is None:
            logging.info('Use default age threshold of 90 days')
            age_threshold = 90
        else:
            age_threshold = args.age_threshold
        detector.check_deletable_samples(age_threshold)

    return 0


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--age_threshold', type=int)
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--final_deletion', action='store_true', default=False,
                        help="set default age threshold to 365 days")

    return parser.parse_args()


if __name__ == '__main__':
    sys.exit(main())
