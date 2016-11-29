import json
import os
import sys
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
FILTER_PROJECT = 'project_id'
FILTER_PLATE = 'plate'
FILTER_RUN = 'run_ids'
FILTERS = [FILTER_FINISHED, FILTER_PROJECT, FILTER_PLATE, FILTER_RUN]

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


def update_samples(cached_samples):
    tmp_samples = defaultdict(dict)
    for s in cached_samples:
        tmp_samples[s.get('sample_id')] = s
    print('Update samples from REST API')
    samples = rest_communication.get_documents('aggregate/samples', paginate=False)
    for s in samples:
        tmp_samples[s.get('sample_id')].update(s)

    required_fields = ['expected_yield', 'plate_name']
    sample_to_update = []
    for s in tmp_samples.values():
        if len(set(s.keys()).intersection(set(required_fields))) < len(required_fields):
            sample_to_update.append(s.get('sample_id'))

    print('get %s samples from lims' % len(sample_to_update))
    lims_samples = get_list_of_samples(sample_to_update)
    _get_artifacts_and_containers_from_samples(lims_samples)
    for lims_sample in lims_samples:
        expected_yield = lims_sample.udf.get('Yield for Quoted Coverage (Gb)', 95)
        tmp_samples[sanitize_user_id(lims_sample.name)]['expected_yield'] = expected_yield
        tmp_samples[sanitize_user_id(lims_sample.name)]['plate_name'] = lims_sample.artifact.container.name

    return list(tmp_samples.values())


def get_runs():
    return None

def update_cache(cached_file, update_target='all'):
    samples, runs = ([], [])
    if os.path.exists(cached_file):
        samples, runs = load_cache(cached_file)
    if update_target in ['all', 'sample']:
        samples = update_samples(samples)
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
    if not status or status == 'reprocess':
        if float(sample['expected_yield']) > float(sample['clean_yield_q30']):
            return STATUS_NEW
        else:
            return STATUS_READY
    if status == 'processing':
        return STATUS_PROCESSING
    if status == 'failed':
        return STATUS_FAILED
    if status == 'finished':
        return STATUS_FINISHED
    return status

def test_filter_aggregate(row_head, col_values, filter, filter_values):
    if filter == FILTER_FINISHED and \
       sum(col_values.values()) == col_values[STATUS_DELIVERED] + col_values[STATUS_SAMPLE_FAILED]:
        return False
    return True

def keep_sample(sample, filter, filter_values):
    if filter in [FILTER_PROJECT, FILTER_PLATE]:
        if sample.get(filter) in filter_values:
            return True
        else:
            return False
    elif filter in [FILTER_RUN]:
        if len(set(sample.get(filter)).intersection(filter_values)) >  0:
            return True
        else:
            return False
    else:
        return True


def aggregate_samples_per(samples, aggregation_key, filter, filter_values):
    samples_per_aggregate = defaultdict(Counter)
    aggregates = set()
    statuses = set()
    header = [aggregation_key] + STATUSES
    for sample in samples:
        if keep_sample(sample, filter, filter_values):
            status = get_sample_status(sample)
            statuses.add(status)
            if isinstance(sample.get(aggregation_key), list):
                for k in sample.get(aggregation_key):
                    aggregates.add(k)
                    samples_per_aggregate[k][status] += 1
            else:
                aggregates.add(sample.get(aggregation_key))
                samples_per_aggregate[sample.get(aggregation_key)][status] += 1
    rows = []
    for aggregate in sorted(aggregates):
        if test_filter_aggregate(aggregate, samples_per_aggregate[aggregate], filter, filter_values):
            out = [aggregate]
            for status in STATUSES:
                out.append(str(samples_per_aggregate[aggregate][status]))
            rows.append(out)
    return header, rows

def format_table(header, rows):
    column_sizes = [len(h) for h in header]
    column_total = [0 for x in range(len(column_sizes))]
    for ci in range(len(column_sizes)):
        column_sizes[ci] = max([len(r[ci]) for r in rows] + [column_sizes[ci]])
        column_total[ci] = sum([int(r[ci]) for r in rows if str(r[ci]).isdigit() ])
    row_formatter = ' | '.join(['{:>%s}'%cs for cs in column_sizes])

    line_length = sum(column_sizes) + (len(column_sizes)-1)*3
    print(row_formatter.format(*header))
    for r in rows:
        print(row_formatter.format(*r))
    if sum(column_total) > 0 :
        column_total[0]='TOTAL'
        print('-' * line_length)
        print(row_formatter.format(*column_total))

def summarize_sample(sample, header_to_report):
    return [', '.join(sample.get(k)) if isinstance(sample.get(k), list) else str(sample.get(k)) for k in header_to_report]

def show_samples(samples, filter_key, filter_values):
    header_to_report = ['project_id', 'plate_name', 'sample_id', 'run_ids', 'status']
    rows = []
    for sample in samples:
        if keep_sample(sample, filter_key, filter_values):
            sample['status'] = get_sample_status(sample)
            rows.append(summarize_sample(sample, header_to_report))
    return header_to_report, rows

def create_report(report_type, cached_file, filter, filter_values):
    samples, runs = load_cache(cached_file)
    if report_type == 'projects':
        header, rows = aggregate_samples_per(samples, 'project_id', filter, filter_values)
    elif report_type == 'plates':
        header, rows = aggregate_samples_per(samples, 'plate_name', filter, filter_values)
    elif report_type == 'run_id':
        header, rows = aggregate_samples_per(samples, 'run_ids', filter, filter_values)
    elif report_type == 'samples':
        header, rows = show_samples(samples, filter, filter_values)
    format_table(header, rows)


def main():
    args = _parse_args()
    load_config()

    cached_file = os.path.join(os.path.expanduser('~'), '.report_cached.json')

    if not os.path.exists(cached_file) or args.pull:
        update_cache(cached_file)
    if args.report_type:
        create_report(args.report_type, cached_file, filter=args.filter_type, filter_values=args.filter_values)
    else:
        print('Provide a report type with --report_type [projects or plates]')

def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('-r', '--report_type', dest='report_type', type=str, choices=['projects', 'plates', 'samples', 'run_id'])
    p.add_argument('--filter_type', type=str, help='set a filter', choices=FILTERS )
    p.add_argument('--filter_values', type=str, nargs='+', help='Things to keep')
    p.add_argument('--pull', action='store_true', help='Force download and update the cache')
    p.add_argument('--debug', action='store_true', help='override pipeline log level to debug')
    return p.parse_args()


if __name__ == '__main__':
    sys.exit(main())
