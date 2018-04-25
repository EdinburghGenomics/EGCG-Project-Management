import argparse

import logging
import sys
from datetime import timedelta, datetime
from os.path import dirname, abspath

from egcg_core import rest_communication as rc
from egcg_core.clarity import connection
from egcg_core.app_logging import logging_default as log_cfg

sys.path.append(dirname(dirname(abspath(__file__))))
from config import load_config


def weekly_facility_meeting_numbers(meeting_date=None, day_since_last_meeting=7):
    if not meeting_date:
        meeting_date = datetime.now()
        print('Assuming meeting date of %s' % meeting_date.date().isoformat())

    date_format = '%d_%m_%Y_%H:%M:%S'
    ten_days_ago = meeting_date - timedelta(days=day_since_last_meeting + 3)

    # number of runs since last meeting
    runs = rc.get_documents('runs', where={'_created': {'$gte': ten_days_ago.strftime(date_format)}})
    # only keep finished runs
    runs = [run for run in runs if run.get('aggregated').get('yield_in_gb')]

    nb_run = len(runs)

    if nb_run > 0:
        avg_yield = sum([run.get('aggregated').get('yield_in_gb') for run in runs]) / nb_run
        avg_q30 = sum([run.get('aggregated').get('pc_q30') for run in runs]) / nb_run


        pass_count = count = 0
        for run in runs:
            for lane in rc.get_documents('lanes', where={'run_id': run.get('run_id')}):
                if lane.get('aggregated').get('review_statuses')[0] == 'pass':
                    pass_count += 1
                count += 1

        pc_pass = pass_count / count * 100

        pass_count = count = 0
        for run in runs:
            run_pass_count = run_count = 0
            for lane in rc.get_documents('lanes', where={'run_id': run.get('run_id')}):
                if lane.get('aggregated').get('useable_statuses', ['no'])[0] == 'yes':
                    run_pass_count += 1

                run_count += 1

            pass_count += run_pass_count / run_count
            count += 1

            pc_useable = pass_count / count * 100
    else:
        avg_yield = 0
        avg_q30 = 0
        pc_pass = 0
        pc_useable = 0

    res = {
        'nb_run': nb_run,
        'avg_yield': avg_yield,
        'avg_q30': avg_q30,
        'run_pc_pass': pc_pass,
        'run_pc_useable': pc_useable
    }

    seven_days_ago = meeting_date - timedelta(days=7)

    processes = rc.get_documents('analysis_driver_procs', where={"status": "finished", "dataset_type": "sample",
                                                                "end_date": {
                                                                    "$gt": seven_days_ago.strftime(date_format)}})
    nb_sample_processed = len(processes)

    samples = []
    for p in connection().get_processes(type='Sample Review EG 1.0 ST',
                             last_modified=seven_days_ago.isoformat().split('.')[0] + "Z"):
        samples.extend([a.samples[0] for a in p.all_inputs(resolve=True)])

    nb_reviewed = len(samples)

    sample_names = [s.name for s in samples]

    rest_samples = [rc.get_document('samples', where={'sample_id': s}) for s in sample_names]

    pc_pass = len([s for s in rest_samples if s.get('reviewed') == 'pass']) / nb_reviewed * 100
    pc_useable = len([s for s in rest_samples if s.get('useable') == 'yes']) / nb_reviewed * 100

    res['nb_sample_processed'] = nb_sample_processed
    res['nb_reviewed'] = nb_reviewed
    res['sample_pc_pass'] = pc_pass
    res['sample_pc_useable'] = pc_useable

    res['review_errors'] = list(set([(s.get('project_id'), s.get('review_comments')) for s in rest_samples if s.get('reviewed') == 'fail']))

    for k in ['nb_run', 'avg_yield', 'avg_q30', 'run_pc_pass', 'run_pc_useable', 'nb_sample_processed',
              'nb_reviewed', 'sample_pc_pass','sample_pc_useable', 'review_errors']:
        print('%s: %s' % (k, res[k]))


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--debug')
    args = p.parse_args()

    load_config()

    log_cfg.add_handler(logging.StreamHandler(stream=sys.stdout), level=logging.INFO)
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    weekly_facility_meeting_numbers()


if __name__ == '__main__':
    main()
