import json
import os
import sys
import logging
import argparse
from collections import defaultdict, Counter

import re
from egcg_core import rest_communication
from egcg_core.clarity import get_list_of_samples

from config import load_config

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def sanitize_user_id(user_id):
    if isinstance(user_id, str):
        return re.sub("[^\w_\-.]", "_", user_id)

def get_samples():
    samples = rest_communication.get_documents('aggregate/samples', paginate=False)
    sample_names = [s.get('sample_id') for s in samples]
    tmp_samples = {}
    for lims_sample in get_list_of_samples(sample_names):
        req_yield = lims_sample.udf.get('Yield for Quoted Coverage (Gb)', 95) * 1000000000
        tmp_samples[sanitize_user_id(lims_sample.name)]['req_yield'] = req_yield
        tmp_samples[sanitize_user_id(lims_sample.name)]['plate'] = lims_sample.artifact.container.name

    for s in samples:
        s.update(tmp_samples[s.get('sample_id')])
    return samples

def update_cache(cached_file):
    samples = get_samples()
    data = {'samples': samples}
    with open(cached_file, 'w') as open_cache:
        json.dump(data, open_cache)


def load_cache(cached_file):
    with open(cached_file) as open_file:
        data = json.load(open_file)
    data.get('samples'), data.get('runs')

def aggregate_samples_per(samples, aggregation_key):
    samples_per_aggregate = defaultdict(Counter)
    aggregates = set()
    statuses = set()
    for sample in samples:
        samples_per_aggregate[sample.get(aggregation_key)][sample.get('status')] += 1

    for project in sorted(aggregates):
        out = [project]
        for status in statuses:
            out.append(samples_per_aggregate[project][status])

        print('\t'.join(out))

def create_report(report_type, cached_file):
    samples, runs = load_cache(cached_file)
    if report_type == 'projects':
        aggregate_samples_per(samples, 'project_id')
    elif report_type == 'projects':
        aggregate_samples_per(samples, 'plate_id')

def main():
    args = _parse_args()
    load_config()

    cached_file = os.path.join(os.path.expanduser('~'), '.report_cached.json')

    if os.path.exists(cached_file) or args.pull:
        update_cache(cached_file)

    create_report(args.report_type, cached_file)


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('-r', '--report_type', dest='report_type', type=str, choices=['projects', 'plates'])
    p.add_argument('--pull', action='store_true', help='Force download and update the cache')
    p.add_argument('--debug', action='store_true', help='override pipeline log level to debug')
    return p.parse_args()


if __name__ == '__main__':
    sys.exit(main())
