import argparse
import logging
import os
import sys
from datetime import datetime

from egcg_core import rest_communication
from egcg_core.app_logging import logging_default as log_cfg

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bin.confirm_delivery import DeliveredSample
from config import load_config
from data_deletion import ProcessedSample


def check_deletable_samples(project_id=None, age_threshold=None, only_confirm=False):
    match = {'proc_status': 'finished', 'useable': 'yes', 'delivered': 'yes', 'data_deleted': 'none'}
    if project_id:
        match['project_id'] = project_id
    sample_records = rest_communication.get_documents(
        'aggregate/samples',
        quiet=True,
        match=match,
        paginate=False
    )
    for r in sample_records:
        s = ProcessedSample(r)
        ds = DeliveredSample(r.get('sample_id'))
        if s.release_date and old_enough_for_deletion(s.release_date, age_threshold):
            if not only_confirm:
                report_sample(r, s, ds)
            elif not ds.files_missing():
                report_sample(r, s, ds)


def report_sample(r, s, ds):
    print('Project %s sample %s, released %s, download confirmed: %s' % (
        r.get('project_id'), r.get('sample_id'), s.release_date, not bool(ds.files_missing())
    ))


def old_enough_for_deletion(date_run, age_threshold=90):
    year, month, day = date_run.split('-')
    age = datetime.utcnow() - datetime(int(year), int(month), int(day))
    return age.days > age_threshold


def main():
    args = _parse_args()
    load_config()
    log_cfg.add_handler(logging.StreamHandler(stream=sys.stdout))
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    check_deletable_samples(args.project_id, args.age_threshold, args.only_confirm)

    return 0


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', type=str)
    parser.add_argument('--age_threshold', type=int, default=90)
    parser.add_argument('--only_confirm', action='store_true')
    parser.add_argument('--debug', action='store_true')

    return parser.parse_args()


if __name__ == '__main__':
    sys.exit(main())
