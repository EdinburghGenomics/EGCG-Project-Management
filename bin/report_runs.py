import json
import os
import sys
import argparse
from collections import defaultdict, Counter
from egcg_core import rest_communication
from egcg_core.clarity import get_list_of_samples, connection, sanitize_user_id, get_sample

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config

cache = {
    'run_elements_data':{},
    'sample_data':{},
    'lane_data':{},
}


def run_elements_data(run_id):
    if not run_id in cache['run_elements_data']:
        cache['run_elements_data'][run_id] = rest_communication.get_documents('run_elements', where={"run_id": run_id})
    return cache['run_elements_data'][run_id]


def sample_data(sample_id):
    if not sample_id in cache['sample_data']:
        cache['sample_data'][sample_id] = rest_communication.get_document('aggregate/samples', match={"sample_id": sample_id})
    return cache['sample_data'][sample_id]

def lane_data(run_id):
    if not run_id in cache['lane_data']:
        cache['lane_data'][run_id] = rest_communication.get_document('aggregate/run_elements_by_lane', match={"run_id": run_id})
    return cache['lane_data'][run_id]

def samples_from_run(run_id):
    samples = []
    for re in run_elements_data(run_id):
        sample_id = re.get('sample_id')
        if sample_id != 'Undetermined': samples.append(sample_id)
    return samples




def report_runs(run_ids):
    run_ids.sort()
    for run_id in run_ids:
        re_data = run_elements_data(run_id)
        lane_review = defaultdict(set)
        lane_review_comment = defaultdict(set)

        for re in re_data:
            lane_review[re.get('lane')].add(re.get('reviewed'))
            lane_review_comment[re.get('lane')].add(re.get('review_comments'))
        count_failure = 0
        reasons = set()
        for lane in sorted(lane_review):
            if len(lane_review.get(lane)) != 1:
                raise ValueError('More than one review status for lane %s in run %s' % (lane, run_id))
            if lane_review.get(lane).pop() == 'fail':
                count_failure += 1
                reasons.update(lane_review_comment.get(lane).pop()[len('failed due to '):].split(', '))
        message = '%s: %s lanes failed ' % (run_id, count_failure)
        if count_failure > 0:
            message += ' due to %s' % ', '.join(reasons)
        print(message)
    print('\n_____________________________________\n')
    for run_id in run_ids:
        list_repeat = set()
        for sample_id in sorted(samples_from_run(run_id)):
            sdata = sample_data(sample_id)
            lims_sample = get_sample(sample_id)
            if sdata.get('clean_yield_in_gb') < lims_sample.udf.get('Required Yield (Gb)') or \
                sdata.get('clean_yield_q30') < lims_sample.udf.get('Yield for Quoted Coverage (Gb)'):
                list_repeat.add(sample_id + ': ' + sdata.get('proc_status', 'Not processing'))
        if list_repeat:
            print('%s: List repeat' % run_id)
            print('\n'.join(sorted(list_repeat)))
        else:
            print('%s: No repeat' % run_id)













def main():
    args = _parse_args()
    load_config()

    report_runs(args.run_ids)




def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('-r', '--run_ids', dest='run_ids', type=str, nargs='+')
    p.add_argument('--debug', action='store_true', help='override pipeline log level to debug')
    return p.parse_args()


if __name__ == '__main__':
    sys.exit(main())
