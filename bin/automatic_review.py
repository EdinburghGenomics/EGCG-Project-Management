import os
import sys
import yaml
import argparse
import logging
from cached_property import cached_property
from egcg_core.app_logging import AppLogger, logging_default as log_cfg
from egcg_core import rest_communication, clarity
from egcg_core.constants import ELEMENT_REVIEW_COMMENTS
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config

cfg_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'etc', 'review_thresholds.yaml')
review_thresholds = yaml.safe_load(open(cfg_path, 'r'))


class Reviewer(AppLogger):
    reviewable_data = None

    @property
    def cfg(self):
        raise NotImplementedError

    def report(self):
        raise NotImplementedError

    @staticmethod
    def _query(content, parts, ret_default=None):
        top_level = content.copy()
        item = None
        for p in parts:
            item = top_level.get(p)
            if item is None:
                return ret_default
            top_level = item
        return item

    def get_failing_metrics(self):
        passfails = {}

        for metric in self.cfg:
            metric_value = self._query(self.reviewable_data, metric.split('.'))
            comparison = self.cfg[metric]['comparison']
            compare_value = self.cfg[metric]['value']

            check = None
            if metric_value is None:
                check = False

            elif comparison == '>':
                check = metric_value >= compare_value

            elif comparison == '<':
                check = metric_value <= compare_value

            elif comparison == 'agreeswith':
                check = metric_value in (self.reviewable_data[compare_value['key']], compare_value['fallback'])

            passfails[metric] = 'pass' if check else 'fail'

        return sorted(k for k, v in passfails.items() if v == 'fail')


class LaneReviewer(Reviewer):
    def __init__(self, aggregated_lane):
        self.reviewable_data = aggregated_lane
        self.run_id = aggregated_lane['run_id']
        self.lane_number = aggregated_lane['lane_number']

    @property
    def cfg(self):
        return review_thresholds['run']

    def report(self):
        failing_metrics = self.get_failing_metrics()
        if failing_metrics:
            report = {
                'reviewed': 'fail',
                ELEMENT_REVIEW_COMMENTS: 'failed due to ' + ', '.join(failing_metrics)
            }
        else:
            report = {'reviewed': 'pass'}

        self.info('%s %s: %s', self.run_id, self.lane_number, report)
        return report

    def push_review(self):
        rest_communication.patch_entries(
            'run_elements',
            payload=self.report(),
            where={'run_id': self.run_id, 'lane': self.lane_number}
        )


class RunReviewer:
    def __init__(self, run_id):
        lanes = rest_communication.get_documents(
            'aggregate/run_elements_by_lane',
            match={'run_id': run_id}
        )
        self.lane_reviewers = [LaneReviewer(lane) for lane in lanes]

    def report(self):
        for r in self.lane_reviewers:
            r.report()

    def push_review(self):
        for r in self.lane_reviewers:
            r.push_review()


class SampleReviewer(Reviewer):
    coverage_values = {30: (40, 30), 95: (120, 30), 120: (160, 40), 190: (240, 60), 270: (360, 90)}

    def __init__(self, sample):
        self.reviewable_data = sample
        self.sample_name = self.reviewable_data['sample_id']
        self.sample_genotype = self.reviewable_data.get('genotype_validation')
        self.species = clarity.get_species_from_sample(self.sample_name)

    @cached_property
    def cfg(self):
        cfg = review_thresholds['sample'].get(self.species, {}).copy()
        cfg.update(review_thresholds['sample']['default'])

        if self.sample_genotype is None:
            self.debug('No genotype validation to review for %s', self.sample_name)
            cfg.pop('genotype_validation.mismatching_snps', None)
            cfg.pop('genotype_validation.no_call_seq', None)

        yieldq30 = clarity.get_expected_yield_for_sample(self.sample_name)
        if not yieldq30:
            self.warning('No yield for quoted coverage found for sample %s', self.sample_name)
            return None

        yieldq30 = int(yieldq30 / 1000000000)
        expected_yield, coverage = self.coverage_values[yieldq30]
        cfg['clean_yield_q30']['value'] = yieldq30
        cfg['clean_yield_in_gb']['value'] = expected_yield
        cfg['median_coverage']['value'] = coverage
        return cfg

    def report(self):
        failing_metrics = self.get_failing_metrics()

        if failing_metrics:
            r = 'fail'
        elif self.species == 'Homo sapiens' and self.sample_genotype is None:
            r = 'genotype missing'
        else:
            r = 'pass'

        report = {'reviewed': r}
        if failing_metrics:
            report[ELEMENT_REVIEW_COMMENTS] = 'failed due to ' + ', '.join(failing_metrics)

        self.info('%s: %s', self.sample_name, report)
        return report

    def push_review(self):
        if self.cfg:
            rest_communication.patch_entry('samples', self.report(), 'sample_id', self.sample_name)


def main():
    args = _parse_args()
    load_config()
    log_cfg.add_handler(logging.StreamHandler(stream=sys.stdout))
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    if args.run_review:
        unreviewed_data = rest_communication.get_documents(
            'aggregate/all_runs',
            paginate=False,
            match={'proc_status': 'finished', 'review_statuses': 'not reviewed'}
        )
        cls = RunReviewer
    elif args.sample_review:
        unreviewed_data = rest_communication.get_documents(
            'aggregate/samples',
            paginate=False,
            match={'proc_status': 'finished', 'reviewed': 'not reviewed'}
        )
        cls = SampleReviewer
    else:
        return 1

    for d in unreviewed_data:
        reviewer = cls(d)
        if args.dry_run:
            reviewer.report()
        else:
            reviewer.push_review()

    return 0


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry_run', action='store_true')
    parser.add_argument('--debug', action='store_true')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--run_review', action='store_true')
    group.add_argument('--sample_review', action='store_true')
    return parser.parse_args()


if __name__ == '__main__':
    sys.exit(main())
