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
        get_reviewable_runs()
    elif args.sample_review:
        get_reviewable_samples()


def _parse_args():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--run_review', action='store_true')
    group.add_argument('--sample_review', action='store_true')
    return parser.parse_args()


def query(content, parts, top_level=None, ret_default=None):
    if top_level is None:
        top_level = content
    item = None
    for p in parts:
        item = top_level.get(p)
        if item != None:
            top_level = item
        else:
            return ret_default
    return item


def sample_config(sample, species):
    coverage_values = {30: (40, 30), 95: (120, 30), 120: (160, 40), 190: (240, 60), 270: (360, 90)}

    sample_cfg = {}
    default_cfg = review_thresholds.get('sample').get('default')
    human_cfg = review_thresholds.get('sample').get('homo_sapiens')
    if species == 'Homo sapiens':
        sample_cfg.update(default_cfg)
        sample_cfg.update(human_cfg)

        if sample.get('genotype_validation') is None:
            del sample_cfg['genotype_validation,mismatching_snps']
            del sample_cfg['genotype_validation,no_call_seq']

    else:
        sample_cfg.update(default_cfg)
    sample_id = sample.get('sample_id')
    yieldq30 = clarity.get_expected_yield_for_sample(sample_id)
    if yieldq30:
        yieldq30 = int(yieldq30 / 1000000000)
        expected_yield, coverage = coverage_values.get(yieldq30)
        sample_cfg['clean_yield_q30']['value'] = yieldq30
        sample_cfg['clean_yield_in_gb']['value'] = expected_yield
        sample_cfg['median_coverage']['value'] = coverage
        return sample_cfg


def morethan(a, b):
    if a is not None:
        return a >= b
    else:
        return False


def lessthan(a, b):
    if a is not None:
        return a <= b
    else:
        return False


def inlist(a, b):
    if a is not None:
        return a in b


def metrics(dataset, cfg):
    PassFailDict = {}
    return_failed_metrics = None
    for metric in (cfg):
        metric_value = (query(dataset, metric.split(',')))
        metric_name = (metric.split(',')[-1])
        if (cfg.get(metric)['comparison']) == '>':
            if morethan(metric_value, (cfg.get(metric)['value'])):
                PassFailDict[metric_name] = 'pass'
            else:
                PassFailDict[metric_name] = 'fail'
        elif (cfg.get(metric)['comparison']) == '<':
            if lessthan(metric_value, (cfg.get(metric)['value'])):
                PassFailDict[metric_name] = 'pass'
            else:
                PassFailDict[metric_name] = 'fail'
        elif (cfg.get(metric)['comparison']) == 'inlist':
            values = (cfg.get(metric)['value'])
            values_list = []
            for v in values:
                value = (query(dataset, [v], ret_default=[v]))
                values_list.append(''.join(value))
            check_metric = (list(set(values_list)))
            if inlist(metric_value, check_metric):
                PassFailDict[metric_name] = 'pass'
            else:
                PassFailDict[metric_name] = 'fail'

    if all(value == 'pass' for value in PassFailDict.values()):
        return ('pass', None)
    else:
        failed_metrics = ([value for value in PassFailDict if PassFailDict[value] == 'fail'])
        return_failed_metrics = ('fail', failed_metrics)
    return return_failed_metrics

class AutomaticReview():

    def __init__(self, cfg, endpoint):
        self.cfg = cfg
        self.endpoint = endpoint

    def patch_entries(self, payload):
        rest_communication.patch_entries(self.enpoint, payload=payload, update_lists=None,
                                         where={"run_id": self.run_name, "lane": lane_number})

        for lane in self.run_element_by_lane:
            lane_number = (lane.get('lane_number'))
            lane_review, reasons = metrics(lane, self.cfg)
            rest_communication.patch_entries('run_elements', payload={'reviewed': lane_review}, update_lists=None,
                                             where={"run_id": self.run_name, "lane": lane_number})
            if reasons:
                rest_communication.patch_entries('run_elements', payload={
                    ELEMENT_REVIEW_COMMENTS: 'failed due to ' + ', '.join(reasons)}, update_lists=None,
                                                 where={"run_id": self.run_name, "lane": lane_number})

            return lane_review



class AutomaticRunReview():
    def __init__(self, run_name, run_element_by_lane, cfg):
        self.run_name = run_name
        self.run_element_by_lane = run_element_by_lane
        self.cfg = cfg

    def patch_entry(self):

        for lane in self.run_element_by_lane:
            lane_number = (lane.get('lane_number'))
            lane_review, reasons = metrics(lane, self.cfg)
            rest_communication.patch_entries('run_elements', payload={'reviewed': lane_review}, update_lists=None,
                                             where={"run_id": self.run_name, "lane": lane_number})
            if reasons:
                rest_communication.patch_entries('run_elements', payload={
                    ELEMENT_REVIEW_COMMENTS: 'failed due to ' + ', '.join(reasons)}, update_lists=None,
                                                 where={"run_id": self.run_name, "lane": lane_number})
            return lane_review


class AutomaticSampleReview():
    def __init__(self, sample, cfg, species):
        self.sample = sample
        self.sample_name = self.sample.get('sample_id')
        self.sample_genotype = self.sample.get('genotype_validation')
        self.cfg = cfg
        self.species = species

    def patch_entry(self):
        sample_review, reasons = metrics(self.sample, self.cfg)
        if all([sample_review == 'pass', self.sample_genotype is None, self.species == 'Homo sapiens']):
            sample_review = 'genotype missing'
        rest_communication.patch_entries('samples', payload={'reviewed': sample_review}, update_lists=None,
                                         where={"sample_id": self.sample_name})
        if reasons:
            rest_communication.patch_entries('samples',
                                             payload={ELEMENT_REVIEW_COMMENTS: 'failed due to ' + ', '.join(reasons)},
                                             update_lists=None, where={"sample_id": self.sample_name})
        return sample_review


def get_reviewable(endpoint, object_id):
    objs = rest_communication.get_documents(endpoint, paginate=False,
                                            match={"proc_status": "finished", "review_statuses": "not%20reviewed"})
    if objs:
        for obj in objs:
            obj_id = obj.get(object_id)
            run_elements_by_lane = rest_communication.get_documents('aggregate/run_elements_by_lane',
                                                                    match={"run_id": obj_id})
            run_cfg = review_thresholds.get('run')
            r = AutomaticRunReview(run_id, run_elements_by_lane, run_cfg)
            r.patch_entry()

def get_reviewable_runs():
    runs = rest_communication.get_documents('aggregate/all_runs', paginate=False,
                                            match={"proc_status": "finished", "review_statuses": "not%20reviewed"})
    if runs:
        for run in runs:
            run_id = run.get('run_id')
            run_elements_by_lane = rest_communication.get_documents('aggregate/run_elements_by_lane',
                                                                    match={"run_id": run_id})
            run_cfg = review_thresholds.get('run')
            r = AutomaticRunReview(run_id, run_elements_by_lane, run_cfg)
            r.patch_entry()


def get_reviewable_samples():
    samples = rest_communication.get_documents('aggregate/samples', paginate=False,
                                               match={"proc_status": "finished", "reviewed": "not%20reviewed"})
    if samples:
        for sample in samples:
            sample_id = sample.get('sample_id')
            species = clarity.get_species_from_sample(sample_id)
            sample_cfg = sample_config(sample, species)
            if sample_cfg:
                s = AutomaticSampleReview(sample, sample_cfg, species)
                s.patch_entry()


if __name__ == '__main__':
    sys.exit(main())
