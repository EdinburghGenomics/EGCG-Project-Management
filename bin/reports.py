import json
import os
import sys
import logging
import argparse
from collections import defaultdict, Counter
import re
from egcg_core import rest_communication
from egcg_core.clarity import get_list_of_samples, connection

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import load_config

STATUS_NEW = 'Not enough data'
STATUS_READY = 'Queued for processing'
STATUS_PROCESSING = 'In pipeline'
STATUS_FAILED = 'Failed pipeline'
STATUS_FINISHED = 'To review'
STATUS_READY_DELIVERED = 'Ready to delivered'
STATUS_SAMPLE_FAILED = 'Do not deliver'
STATUS_DELIVERED = 'Delivered'

FILTER_FINISHED = 'all_finished'

STATUSES = [STATUS_NEW, STATUS_READY, STATUS_PROCESSING, STATUS_FAILED, STATUS_FINISHED, STATUS_SAMPLE_FAILED,
            STATUS_READY_DELIVERED, STATUS_DELIVERED]

def sanitize_user_id(user_id):
    if isinstance(user_id, str):
        return re.sub("[^\w_\-.]", "_", user_id)


def _get_artifacts_and_containers_from_samples(samples):
    artifacts = [s.artifact for s in samples]
    lims = connection()
    print('retrieve %s artifacts'%len(artifacts))
    for start in range(0, len(artifacts), 100):
        lims.get_batch(artifacts[start:start + 100])

def get_samples():
    print('retrieve samples from REST API')
    samples = rest_communication.get_documents('aggregate/samples', paginate=False)
    sample_names = [s.get('sample_id') for s in samples]
    tmp_samples = defaultdict(dict)
    print('get %s samples from lims'%len(sample_names))
    lims_samples = get_list_of_samples(sample_names)
    _get_artifacts_and_containers_from_samples(lims_samples)
    print('get other info from lims')
    for lims_sample in lims_samples:
        req_yield = lims_sample.udf.get('Yield for Quoted Coverage (Gb)', 95)
        tmp_samples[sanitize_user_id(lims_sample.name)]['req_yield'] = req_yield
        tmp_samples[sanitize_user_id(lims_sample.name)]['plate'] = lims_sample.artifact.container.name

    for s in samples:
        s.update(tmp_samples[s.get('sample_id')])
    return samples

def get_runs():
    return None

def update_cache(cached_file, update_target='all'):
    samples, runs = load_cache(cached_file)
    if update_target in ['all', 'sample']:
        samples = get_samples()
    if update_target in ['all', 'run']:
        runs = get_runs()

    data = {'samples': samples, 'runs': runs}
    with open(cached_file, 'w') as open_cache:
        json.dump(data, open_cache)


def load_cache(cached_file):
    with open(cached_file) as open_file:
        data = json.load(open_file)
    return data.get('samples'), data.get('runs')


def get_sample_status(sample):
    if sample['delivered'] == 'yes':
        return STATUS_DELIVERED
    if sample['useable'] == 'yes':
        return STATUS_READY_DELIVERED
    if sample['useable'] == 'no':
        return STATUS_SAMPLE_FAILED
    status = sample.get('proc_status')
    if not status or status == 'reprocessed':
        if sample['req_yield'] < sample['clean_yield_q30']:
            return STATUS_NEW
        else:
            return STATUS_READY
    if status == 'processing':
        return STATUS_PROCESSING
    if status == 'failed':
        return STATUS_FAILED
    if status == 'finished':
        return STATUS_FINISHED

def test_filter(row_head, col_values, filter):
    if filter == FILTER_FINISHED and \
       sum(col_values.values()) == col_values[STATUS_DELIVERED] + col_values[STATUS_SAMPLE_FAILED]:
        return False
    return True

def aggregate_samples_per(samples, aggregation_key, filter=None):
    samples_per_aggregate = defaultdict(Counter)
    aggregates = set()
    statuses = set()
    header = [aggregation_key] + STATUSES
    for sample in samples:
        status = get_sample_status(sample)
        statuses.add(status)
        aggregates.add(sample.get(aggregation_key))
        samples_per_aggregate[sample.get(aggregation_key)][status] += 1
    rows = []
    for aggregate in sorted(aggregates):
        if test_filter(aggregate, samples_per_aggregate[aggregate], filter):
            out = [aggregate]
            for status in STATUSES:
                out.append(str(samples_per_aggregate[aggregate][status]))
            rows.append(out)
    return header, rows

def format_table(header, rows):
    table = [header] + rows
    column_sizes = [len(h) for h in header]
    for ci in range(len(column_sizes)):
        column_sizes[ci] = max([len(r[ci]) for r in rows] + [column_sizes[ci]])
    row_formatter = ' | '.join(['{:>%s}'%cs for cs in column_sizes])

    print(row_formatter.format(*header))
    for r in rows:
        print(row_formatter.format(*r))

def create_report(report_type, cached_file, filter):
    samples, runs = load_cache(cached_file)
    if report_type == 'projects':
        header, rows = aggregate_samples_per(samples, 'project_id', filter)
    elif report_type == 'plates':
        header, rows = aggregate_samples_per(samples, 'plate', filter)
    format_table(header, rows)


def main():
    args = _parse_args()
    load_config()

    cached_file = os.path.join(os.path.expanduser('~'), '.report_cached.json')

    if not os.path.exists(cached_file) or args.pull:
        update_cache(cached_file)

    create_report(args.report_type, cached_file, filter=args.filter)


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('-r', '--report_type', dest='report_type', type=str, choices=['projects', 'plates'])
    p.add_argument('--filter', type=str, help='set a filter', choices=[FILTER_FINISHED] )
    p.add_argument('--pull', action='store_true', help='Force download and update the cache')
    p.add_argument('--debug', action='store_true', help='override pipeline log level to debug')
    return p.parse_args()


if __name__ == '__main__':
    sys.exit(main())
