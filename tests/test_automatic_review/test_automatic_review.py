from os.path import join, dirname, abspath

from egcg_core.config import Configuration, EnvConfiguration

from automatic_review import automatic_review
from tests import TestProjectManagement
from tests.test_automatic_review import fake_data


def merge(original_dict, override_dict):
    mydict = dict(EnvConfiguration._merge_dicts(original_dict, override_dict))
    return mydict


class TestAutomaticReview(TestProjectManagement):
    def test_run_review(self):

        cfg = Configuration([join(dirname(dirname(abspath(__file__))), '..', 'etc', 'review_thresholds.yaml')])
        cfg = cfg.get('run')

        for lane in fake_data.run_elements_by_lane_pass:
            run_review = automatic_review.metrics(lane, cfg)
            assert run_review == ['pass']
        for lane in fake_data.run_elements_by_lane_fail:
            run_review = automatic_review.metrics(lane, cfg)
            assert run_review[0] == 'fail'

    def test_sample_review(self):



        cfg = Configuration([join(dirname(dirname(abspath(__file__))), '..', 'etc', 'review_thresholds.yaml')])
        default = (cfg.get('sample').get('default'))
        homo_sapiens = (cfg.get('sample').get('homo_sapiens'))
        my_cfg = {}
        my_cfg.update(default)
        my_cfg.update(homo_sapiens)
        print(my_cfg)

        sample_review = automatic_review.metrics(fake_data.samples_fail, my_cfg)
        review = sample_review[0]
        failed_fields = sample_review[1]
        assert review == 'fail'
        assert set(failed_fields) == {'clean_yield_q30', 'clean_yield_in_gb'}

        sample_review = automatic_review.metrics(fake_data.samples_pass, my_cfg)
        review = sample_review[0]
        assert review == 'pass'