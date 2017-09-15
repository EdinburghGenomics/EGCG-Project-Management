import argparse
import csv
import logging
import operator
import os
import sys
from collections import defaultdict
from datetime import datetime

from egcg_core import rest_communication, clarity
from egcg_core.app_logging import logging_default as log_cfg

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import load_config
from egcg_core.notifications import send_email
from egcg_core.config import cfg

def _utcnow():
    return datetime.utcnow()

def old_enough_for_deletion(date_run, age_threshold=90):
    year, month, day = date_run.split('-')
    age = _utcnow() - datetime(int(year), int(month), int(day))
    return age.days > age_threshold


def download_confirmation(sample_data):
    # TODO: need to check the LIMS for download confirmation when implemented there
    # for now look only at the files downloaded
    file_downloaded = set([f['file_path'] for f in sample_data.get('files_downloaded', [])])
    files_delivered = sample_data.get('files_delivered', [])
    file_missing = [f['file_path'] for f in files_delivered if f['file_path'] not in file_downloaded]
    return not bool(file_missing)


def check_deletable_samples(age_threshold=None):
    where = {'useable': 'yes', 'delivered': 'yes', 'data_deleted': 'none'}
    sample_records = rest_communication.get_documents(
        'samples',
        quiet=True,
        where=where,
        all_pages=True,
        max_results=100
    )
    if age_threshold is None:
        age_threshold = cfg.query('data_deletion', 'age_threshold')

    project_batches = defaultdict(list)
    for r in sample_records:
        release_date = clarity.get_sample_release_date(r.get('sample_id'))
        confirmation = download_confirmation(r)
        if release_date and old_enough_for_deletion(release_date, age_threshold):
            pb = (r.get('project_id'), release_date)
            project_batches[pb].append((r.get('sample_id'), confirmation))

    today = _utcnow().strftime('%Y-%m-%d')
    output_dir = cfg.query('data_deletion', 'log_dir')
    if not output_dir or not os.path.exists(output_dir):
        output_dir = os.getcwd()
    output_file = os.path.join(output_dir, 'Candidate_samples_for_deletion_gt_%s_days_old_%s.csv' % (age_threshold, today))
    write_report(project_batches, output_file)
    subject = 'Samples ready for deletion'
    msg = '''Hi,
The attached csv file contains all samples ready for deletion on the {today}.
Please review them and get back to the bioinformatics team with samples that can be deleted.
'''.format(today=today)
    send_email(
        msg=msg,
        subject=subject,
        attachments=[output_file],
        strict=True,
        **cfg['data_deletion']['email_notification']
    )


def write_report(project_batches, output_file):
    # format report
    headers = ['Project id', 'Release date', 'Nb sample confirmed', 'Nb sample not confirmed',
               'Download not confirmed', 'Download confirmed']
    with open(output_file, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(headers)

        #sort by release date
        batch_keys = sorted(project_batches, key=operator.itemgetter(1))
        for pb in batch_keys:
            project_id, release_date = pb
            out = [project_id, release_date]
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
    load_config()
    log_cfg.add_handler(logging.StreamHandler(stream=sys.stdout))
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    check_deletable_samples(args.age_threshold)

    return 0


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--age_threshold', type=int)
    parser.add_argument('--debug', action='store_true')

    return parser.parse_args()


if __name__ == '__main__':
    sys.exit(main())
