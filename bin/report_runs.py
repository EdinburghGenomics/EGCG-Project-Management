import os
import sys
import argparse
from datetime import date
from collections import defaultdict
from egcg_core.config import cfg
from egcg_core.util import query_dict
from egcg_core import rest_communication
from egcg_core.notifications.email import send_html_email

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'etc', 'list_repeats.html'
)


def today():
    return date.today().isoformat()


def run_status_data(run_id):
    if not cache['run_status_data']:
        data = rest_communication.get_documents('lims/status/run_status')
        for d in data:
            cache['run_status_data'][d['run_id']] = d
    return cache['run_status_data'][run_id]


def run_elements_data(run_id):
    if run_id not in cache['run_elements_data']:
        cache['run_elements_data'][run_id] = rest_communication.get_documents('run_elements', where={'run_id': run_id})
    return cache['run_elements_data'][run_id]


def sample_data(sample_id):
    if sample_id not in cache['sample_data']:
        cache['sample_data'][sample_id] = rest_communication.get_document('samples', where={'sample_id': sample_id})
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

    failed_lanes = 0
    reasons = set()
    for lane in sorted(lane_review):
        if len(lane_review.get(lane)) != 1:
            raise ValueError('More than one review status for lane %s in run %s' % (lane, run_id))
        if lane_review.get(lane).pop() == 'fail':
            failed_lanes += 1
            reasons.update(
                lane_review_comment.get(lane).pop()[len('failed due to '):].split(', ')
            )

    reasons = sorted(reasons)
    message = '%s: %s lanes failed ' % (run_id, failed_lanes)
    run_info['failed_lanes'] = failed_lanes
    if failed_lanes > 0:
        message += ' due to %s' % ', '.join(reasons)
    run_info['details'] = ', '.join(reasons)
    print(message)
    return run_info


def report_runs(run_ids, noemail=False):
    run_ids.sort()

    runs_info = []
    for run_id in run_ids:
        run_status = run_status_data(run_id).get('run_status')
        if run_status == 'RunCompleted':
            run_info = get_run_success(run_id)
        else:
            print('%s: 8 lanes failed due to %s' % (run_id, run_status))
            run_info = {'name': run_id, 'failed_lanes': 8, 'details': str(run_status)}
        runs_info.append(run_info)

    print('\n_____________________________________\n')

    run_repeats = []
    for run_id in run_ids:

        sample_repeats = []
        for sample_id in sorted(samples_from_run(run_id)):
            sdata = sample_data(sample_id) or {}

            proc_status = query_dict(sdata, 'aggregated.most_recent_proc.status') or 'not processing'
            clean_pc_q30 = query_dict(sdata, 'aggregated.clean_pc_q30') or 0
            clean_yield_in_gb = query_dict(sdata, 'aggregated.clean_yield_in_gb') or 0
            clean_yield = clean_yield_in_gb * 1000000000
            mean_cov = query_dict(sdata, 'aggregated.from_run_elements.mean_coverage') or 0

            if clean_pc_q30 >= 75 and (clean_yield >= sdata['required_yield'] or mean_cov >= sdata['required_coverage']):
                pass
            else:
                reason = 'unknown'
                if not clean_pc_q30:
                    reason = 'No data'
                elif clean_pc_q30 < 75:
                    reason = 'Low quality'
                elif clean_yield < sdata['required_yield'] and mean_cov < sdata['required_coverage']:
                    reason = 'Not enough data'

                sample_repeats.append({'id': sample_id, 'reason': reason + ': ' + proc_status})

        sample_repeats.sort(key=lambda s: s['id'])

        if sample_repeats:
            print('%s: Repeat samples' % run_id)
            for s in sample_repeats:
                print('%s: %s' % (s['id'], s['reason']))
        else:
            print('%s: No repeat samples' % run_id)

        run_repeats.append({'name': run_id, 'repeat_count': len(sample_repeats), 'repeats': sample_repeats})

    if noemail:
        return

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
    params['runs'] = run_repeats
    send_html_email(
        subject='Sequencing repeats %s' % _today,
        email_template=email_template_repeats,
        **params
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument('-r', '--run_ids', dest='run_ids', type=str, nargs='+')
    p.add_argument('--debug', action='store_true', help='override pipeline log level to debug')
    p.add_argument('--noemail', action='store_true')
    args = p.parse_args()
    load_config()

    report_runs(args.run_ids, args.noemail)


if __name__ == '__main__':
    sys.exit(main())
