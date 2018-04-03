import json
import os
import sys
import argparse
from collections import defaultdict, Counter

from datetime import date
from egcg_core import rest_communication
from egcg_core.notifications.email import send_html_email

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from egcg_core.config import cfg
from config import load_config

cache = {
    'run_elements_data': {},
    'sample_data': {},
    'run_status_data': {}
}

email_template_report = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'etc', 'run_report.html'
)

email_template_repeats = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'etc', 'list_repeat.html'
)


def today():
    return date.today().isoformat()


def run_status_data(run_id):
    if not cache['run_status_data']:
        data = rest_communication.get_documents('lims/status/run_status')
        for d in data:
            cache['run_status_data'][d.get('run_id')] = d
    return cache['run_status_data'][run_id]


def run_elements_data(run_id):
    if not run_id in cache['run_elements_data']:
        cache['run_elements_data'][run_id] = rest_communication.get_documents('run_elements', where={"run_id": run_id})
    return cache['run_elements_data'][run_id]


def sample_data(sample_id):
    if not sample_id in cache['sample_data']:
        cache['sample_data'][sample_id] = rest_communication.get_document('samples', where={"sample_id": sample_id})
    return cache['sample_data'][sample_id]


def samples_from_run(run_id):
    return run_status_data(run_id).get('sample_ids')


def get_run_success(run_id):
    run_info = {'name': run_id}
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
            reasons.update(
                lane_review_comment.get(lane).pop()[len('failed due to '):].split(', ')
            )
    reasons = sorted(reasons)
    message = '%s: %s lanes failed ' % (run_id, count_failure)
    run_info['count_fail'] = count_failure
    if count_failure > 0:
        message += ' due to %s' % ', '.join(reasons)
    run_info['details'] = ', '.join(reasons)
    print(message)
    return run_info


def report_runs(run_ids):
    run_ids.sort()
    runs_info = []
    for run_id in run_ids:
        run_status = run_status_data(run_id).get('run_status')
        if run_status == 'RunCompleted':
            run_info = get_run_success(run_id)
        else:
            print('%s: 8 lanes failed due to %s' % (run_id, run_status))
            run_info = {'name': run_id, 'count_fail': 8, 'details': '%s' % run_status}
        runs_info.append(run_info)

    print('\n_____________________________________\n')

    runs_repeats = []
    for run_id in run_ids:

        list_repeat = set()
        samples_fail = []
        for sample_id in sorted(samples_from_run(run_id)):

            sdata = sample_data(sample_id)
            proc_status = 'Not processing'
            if sdata and sdata.get('aggregated', {}) and sdata.get('aggregated', {}).get('most_recent_proc', {}):
                proc_status = sdata.get('aggregated', {}).get('most_recent_proc', {}).get('status', 'Not processing')
            if not sdata or not sdata.get('aggregated').get('clean_pc_q30'):
                list_repeat.add(sample_id + ': No data')
                samples_fail.append({'id': sample_id, 'reason': 'No data'})
            elif sdata.get('aggregated').get('clean_pc_q30') < 75:
                list_repeat.add(sample_id + ': Low quality (%s)' % proc_status)
                samples_fail.append({'id': sample_id, 'reason': 'Low quality'})
            elif sdata.get('aggregated').get('clean_yield_in_gb') * 1000000000 < sdata.get('required_yield') and \
                 sdata.get('aggregated').get('from_run_elements').get('mean_coverage') < sdata.get('required_coverage'):
                list_repeat.add(sample_id + ': Not enough data (%s)' % proc_status)
                samples_fail.append({'id': sample_id, 'reason': 'Not enough data'})
        if list_repeat:
            print('%s: List repeat' % run_id)
            print('\n'.join(sorted(list_repeat)))
        else:
            print('%s: No repeat' % run_id)
        runs_repeats.append({'name': run_id, 'count_repeat': len(samples_fail), 'sample_list': samples_fail})

    _today = today()

    params = {}
    params.update(cfg['run_report']['email_notification'])
    params['runs'] = runs_info
    send_html_email(
        subject='Run report %s' % _today,
        email_template=email_template_report,
        **params
    )

    params = {}
    params.update(cfg['run_report']['email_notification'])
    params['runs'] = runs_repeats
    send_html_email(
        subject='List of repeat %s' % _today,
        email_template=email_template_repeats,
        **params
    )


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
