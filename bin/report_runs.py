import os
import sys
import argparse
from datetime import date
from collections import defaultdict

from egcg_core.app_logging import logging_default
from egcg_core.config import cfg
from egcg_core.util import query_dict
from egcg_core import rest_communication
from egcg_core.notifications.email import send_html_email

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config

cache = {
    'run_elements_data': {},
    'run_data': {},
    'lanes_data': {},
    'sample_data': {},
    'run_status_data': {}
}

email_template_report = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'etc', 'run_report.html'
)

email_template_repeats = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'etc', 'list_repeats.html'
)

logging_default.add_stdout_handler()
logger = logging_default.get_logger(os.path.basename(__file__))


def today():
    return date.today().isoformat()


def run_status_data(run_id):
    if not cache['run_status_data']:
        data = rest_communication.get_documents('lims/status/run_status')
        for d in data:
            cache['run_status_data'][d['run_id']] = d
    return cache['run_status_data'][run_id]


def run_data(run_id):
    if run_id not in cache['run_data']:
        cache['run_data'][run_id] = rest_communication.get_document('runs', where={'run_id': run_id})
    return cache['run_data'][run_id]


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
    reasons = []
    for lane in sorted(lane_review):
        if len(lane_review.get(lane)) != 1:
            raise ValueError('More than one review status for lane %s in run %s' % (lane, run_id))
        if lane_review.get(lane).pop() == 'fail':
            failed_lanes += 1
            reasons.append(
                'lane %s: %s' % (lane, lane_review_comment.get(lane).pop()[len('failed due to '):])

            )

    reasons = sorted(reasons)
    message = '%s: %s lanes failed' % (run_id, failed_lanes)
    run_info['failed_lanes'] = failed_lanes
    if failed_lanes > 0:
        message += ':\n%s' % '\n'.join(reasons)
    run_info['details'] = reasons
    for l in message.split('\n'):
        logger.info(l)
    return run_info


def check_pending_run_element(sample_id, sdata):
    # Checking for other run elements which are still pending
    for sample_run_element in query_dict(sdata, 'run_elements') or []:
        # Splitting the run element, and generating the run_id by concatenating the first four components
        # with an underscore
        sample_run_id = '_'.join(sample_run_element.split('_')[:4])
        if query_dict(run_data(sample_run_id), 'aggregated.most_recent_proc.status') == 'processing':
            logger.info('Another pending run element already exists for sample ' + sample_id)
            return True
    return False


def remove_duplicate_base_on_flowcell_id(list_runs):
    """
    Take a list of runs and remove the duplicated run based on the flowcell id.
    It will remove the oldest run when two are found based on the run date.
    """
    flowcell_to_run = {}
    for run_id in list_runs:
        date, machine, run_number, stage_flowcell = run_id.split('_')
        flowcell = stage_flowcell[1:]
        # If the run id has not been seen or if the date is newer than the previous one then keep it
        if flowcell not in flowcell_to_run or run_id > flowcell_to_run[flowcell]:
            flowcell_to_run[flowcell] = run_id

    return sorted(flowcell_to_run.values())


def report_runs(run_ids, noemail=False):
    run_ids.sort()

    runs_info = []
    for run_id in run_ids:
        run_status = run_status_data(run_id).get('run_status')
        if run_status == 'RunCompleted':
            run_info = get_run_success(run_id)
        else:
            logger.info('%s: 8 lanes failed due to %s' % (run_id, run_status))
            run_info = {'name': run_id, 'failed_lanes': 8, 'details': [str(run_status)]}
        runs_info.append(run_info)

    logger.info('')
    logger.info('_____________________________________')
    logger.info('')

    run_repeats = []
    # Remove the duplicated run from repeated flowcell
    run_ids = remove_duplicate_base_on_flowcell_id(run_ids)

    for run_id in run_ids:

        sample_repeats = []
        for sample_id in sorted(samples_from_run(run_id)):
            sdata = sample_data(sample_id) or {}

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
                elif clean_yield < sdata['required_yield'] and mean_cov < sdata['required_coverage']:
                    reason = 'Not enough data: yield (%s < %s) and coverage (%s < %s)' % (
                        round(clean_yield/1000000000, 1), int(sdata['required_yield']/1000000000),
                        round(mean_cov, 1), sdata['required_coverage']
                    )
                # if a pending run element exists, continue to the next sample without logging current one
                if check_pending_run_element(sample_id, sdata):
                    continue

                sample_repeats.append({'id': sample_id, 'reason': reason})

        sample_repeats.sort(key=lambda s: s['id'])

        if sample_repeats:
            logger.info('%s: Repeat samples' % run_id)
            for s in sample_repeats:
                logger.info('%s: %s' % (s['id'], s['reason']))
        else:
            logger.info('%s: No repeat samples' % run_id)

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
