from os.path import join, dirname, abspath
from egcg_core.config import Configuration, EnvConfiguration
from bin import automatic_review
from tests import TestProjectManagement
from tests.test_automatic_review import fake_data
from unittest.mock import patch


def merge(original_dict, override_dict):
    mydict = dict(EnvConfiguration._merge_dicts(original_dict, override_dict))
    return mydict


class TestAutomaticReview(TestProjectManagement):
    def test_run_review(self):

        cfg = Configuration([join(dirname(dirname(abspath(__file__))), '..', 'etc', 'review_thresholds.yaml')])
        cfg = cfg.get('run')
        for lane in fake_data.run_elements_by_lane_pass:
            run_review, reasons = automatic_review.metrics(lane, cfg)
            assert run_review == 'pass'
        for lane in fake_data.run_elements_by_lane_fail:
            run_review, reasons = automatic_review.metrics(lane, cfg)
            assert run_review == 'fail'



    @patch('bin.automatic_review.clarity.get_species_from_sample')
    @patch('bin.automatic_review.clarity.get_expected_yield_for_sample')
    def test_sample_config(self, mock_yield, mock_species):
        mock_species.return_value = 'Homo sapiens'
        mock_yield.return_value = 120000000000
        no_genotype = automatic_review.sample_config('test', None)

        assert no_genotype == {'clean_yield_in_gb': {'comparison': '>', 'value': 160},
                               'pc_duplicate_reads': {'comparison': '<', 'value': 35},
                               'pc_mapped_reads': {'comparison': '>', 'value': 90},
                               'median_coverage': {'comparison': '>', 'value': 40},
                               'clean_yield_q30': {'comparison': '>', 'value': 120},
                               'provided_gender': {'value': ['unknown', 'called_gender'], 'comparison': 'inlist'}
                               }



        genotype = {'mismatching_snps': 0, 'no_call_seq': 0, 'no_call_chip': 3, 'matching_snps': 29}
        with_genotype = automatic_review.sample_config('test', genotype)
        assert with_genotype == {'clean_yield_in_gb': {'comparison': '>', 'value': 160},
                               'pc_duplicate_reads': {'comparison': '<', 'value': 35},
                               'pc_mapped_reads': {'comparison': '>', 'value': 90},
                               'median_coverage': {'comparison': '>', 'value': 40},
                               'clean_yield_q30': {'comparison': '>', 'value': 120},
                               'provided_gender': {'value': ['unknown', 'called_gender'], 'comparison': 'inlist'},
                                'genotype_validation,no_call_seq': {'comparison': '<', 'value': 10},
                                'genotype_validation,mismatching_snps': {'comparison': '<', 'value': 5}
                               }


        mock_species.return_value = 'Bos taurus'
        mock_yield.return_value = 120000000000
        non_human = automatic_review.sample_config('test', None)
        assert non_human == {'clean_yield_in_gb': {'comparison': '>', 'value': 160},
                        'pc_duplicate_reads': {'comparison': '<', 'value': 35},
                        'pc_mapped_reads': {'comparison': '>', 'value': 90},
                        'median_coverage': {'comparison': '>', 'value': 40},
                        'clean_yield_q30': {'comparison': '>', 'value': 120}}


    @patch('bin.automatic_review.clarity.get_species_from_sample')
    @patch('bin.automatic_review.clarity.get_expected_yield_for_sample')
    def test_metrics(self, mock_yield, mock_species):
        mock_species.return_value = 'Homo sapiens'
        mock_yield.return_value = 95000000000

        genotype = (fake_data.samples_fail.get('genotype_validation'))
        sample_cfg = automatic_review.sample_config('test', genotype)
        sample_review, reasons = automatic_review.metrics(fake_data.samples_fail, sample_cfg)
        assert sample_review == 'fail'
        assert set(reasons) == set(['clean_yield_q30', 'clean_yield_in_gb'])

        genotype = (fake_data.samples_pass.get('genotype_validation'))
        sample_cfg = automatic_review.sample_config('test', genotype)
        sample_review, reasons = automatic_review.metrics(fake_data.samples_pass, sample_cfg)
        assert sample_review == 'pass'

        genotype = (fake_data.samples_no_genotype.get('genotype_validation'))
        sample_cfg = automatic_review.sample_config('test', genotype)
        sample_review, reasons = automatic_review.metrics(fake_data.samples_no_genotype, sample_cfg)
        assert sample_review == 'pass'

