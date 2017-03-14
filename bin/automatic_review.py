import os
import sys
import yaml
import argparse
import logging
from egcg_core.app_logging import logging_default as log_cfg
from egcg_core import rest_communication, clarity
from egcg_core.constants import ELEMENT_REVIEW_COMMENTS
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config

cfg_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'etc', 'review_thresholds.yaml')
review_thresholds = yaml.safe_load(open(cfg_path, 'r'))


def main():
    args = _parse_args()
    load_config()
    log_cfg.default_level = logging.DEBUG
    log_cfg.add_handler(logging.StreamHandler(stream=sys.stdout), logging.DEBUG)
    if args.run_review:
        review_runs()
    elif args.sample_review:
        review_samples()


def _parse_args():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--run_review', action='store_true')
    group.add_argument('--sample_review', action='store_true')
    return parser.parse_args()


def query(content, parts, ret_default=None):
    top_level = content.copy()
    item = None
    for p in parts:
        item = top_level.get(p)
        if item is None:
            return ret_default
        top_level = item
    return item


def sample_config(sample, species):
    coverage_values = {30: (40, 30), 95: (120, 30), 120: (160, 40), 190: (240, 60), 270: (360, 90)}
    sample_cfg = review_thresholds['sample']['default'].copy()

    if species == 'Homo sapiens':
        sample_cfg.update(review_thresholds['sample']['homo_sapiens'])
        if sample.get('genotype_validation') is None:
            del sample_cfg['genotype_validation.mismatching_snps']
            del sample_cfg['genotype_validation.no_call_seq']

    yieldq30 = clarity.get_expected_yield_for_sample(sample['sample_id'])
    if not yieldq30:
        return None

    yieldq30 = int(yieldq30 / 1000000000)
    expected_yield, coverage = coverage_values[yieldq30]
    sample_cfg['clean_yield_q30']['value'] = yieldq30
    sample_cfg['clean_yield_in_gb']['value'] = expected_yield
    sample_cfg['median_coverage']['value'] = coverage
    return sample_cfg


def get_failing_metrics(metrics, cfg):
    passfails = {}

    for metric in cfg:
        metric_value = query(metrics, metric.split('.'))
        comparison = cfg[metric]['comparison']
        compare_value = cfg[metric]['value']

        check = None
        if metric_value is None:
            check = False

        elif comparison == '>':
            check = metric_value >= compare_value

        elif comparison == '<':
            check = metric_value <= compare_value

        elif comparison == 'agreeswith':
            check = metric_value in (metrics[compare_value['key']], compare_value['fallback'])

        result = 'pass' if check else 'fail'
        passfails[metric] = result

    return sorted(k for k, v in passfails.items() if v == 'fail')


class RunReviewer:
    def __init__(self, run_name):
        self.run_name = run_name
        self.run_elements_by_lane = rest_communication.get_documents(
            'aggregate/run_elements_by_lane',
            match={'run_id': self.run_name}
        )

    def patch_entry(self):
        for run_element in self.run_elements_by_lane:
            lane_number = run_element['lane_number']
            failing_metrics = get_failing_metrics(run_element, review_thresholds['run'])

            payload = {'reviewed': 'pass'}
            if failing_metrics:
                payload = {
                    'reviewed': 'fail',
                    ELEMENT_REVIEW_COMMENTS: 'failed due to ' + ', '.join(failing_metrics)
                }

            rest_communication.patch_entries(
                'run_elements',
                payload=payload,
                where={'run_id': self.run_name, 'lane': lane_number}
            )


class SampleReviewer:
    def __init__(self, sample, cfg, species):
        self.sample = sample
        self.sample_name = self.sample['sample_id']
        self.sample_genotype = self.sample.get('genotype_validation')
        self.cfg = cfg
        self.species = species

    def patch_entry(self):
        failing_metrics = get_failing_metrics(self.sample, self.cfg)

        if failing_metrics:
            r = 'fail'
        elif self.species == 'Homo sapiens' and self.sample_genotype is None:
            r = 'genotype missing'
        else:
            r = 'pass'

        payload = {'reviewed': r}
        if failing_metrics:
            payload[ELEMENT_REVIEW_COMMENTS] = 'failed due to ' + ', '.join(failing_metrics)

        rest_communication.patch_entries('samples', payload=payload, where={'sample_id': self.sample_name})


def review_runs():
    unreviewed_runs = rest_communication.get_documents(
        'aggregate/all_runs',
        paginate=False,
        match={'proc_status': 'finished', 'review_statuses': 'not%20reviewed'}
    )
    for run in unreviewed_runs:
        r = RunReviewer(run['run_id'])
        r.patch_entry()


def review_samples():
    unreviewed_samples = rest_communication.get_documents(
        'aggregate/samples',
        paginate=False,
        match={'proc_status': 'finished', 'reviewed': 'not%20reviewed'}
    )
    for sample in unreviewed_samples:
        species = clarity.get_species_from_sample(sample['sample_id'])
        sample_cfg = sample_config(sample, species)
        if sample_cfg:
            s = SampleReviewer(sample, sample_cfg, species)
            s.patch_entry()


if __name__ == '__main__':
    sys.exit(main())
