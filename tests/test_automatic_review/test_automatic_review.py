from bin import automatic_review as ar
from tests.test_automatic_review import fake_data
from tests import TestProjectManagement
from unittest.mock import patch

ppath = 'bin.automatic_review.'


class TestLaneReviewer(TestProjectManagement):
    def setUp(self):
        self.passing_reviewer = ar.LaneReviewer(fake_data.passing_lane)
        self.failing_reviewer_1 = ar.LaneReviewer(fake_data.failing_lanes[0])
        self.failing_reviewer_2 = ar.LaneReviewer(fake_data.failing_lanes[1])

    def test_failing_metrics(self):
        assert self.passing_reviewer.get_failing_metrics() == []
        assert self.failing_reviewer_1.get_failing_metrics() == ['pc_pass_filter', 'yield_in_gb']
        assert self.failing_reviewer_2.get_failing_metrics() == ['pc_pass_filter', 'pc_q30', 'yield_in_gb']

    @patch(ppath + 'LaneReviewer.get_failing_metrics', side_effect=[[], ['some', 'failing', 'metrics']])
    def test_summary(self, mocked_get_failing_metrics):
        assert self.passing_reviewer._summary == {'reviewed': 'pass'}
        assert self.failing_reviewer_1._summary == {
            'reviewed': 'fail', 'review_comments': 'failed due to some, failing, metrics'
        }

    @patch(ppath + 'rest_communication.patch_entries')
    @patch(ppath + 'LaneReviewer._summary', new='a_payload')
    def test_push_review(self, mocked_patch):
        self.passing_reviewer.push_review()
        mocked_patch.assert_called_with(
            'run_elements',
            payload='a_payload',
            where={'run_id': 'a_run', 'lane': 1}
        )


class TestSampleReviewer(TestProjectManagement):
    def setUp(self):
        with patch(ppath + 'clarity.get_species_from_sample', return_value='Homo sapiens'):
            self.passing_reviewer = ar.SampleReviewer(fake_data.passing_sample)
            self.failing_reviewer = ar.SampleReviewer(fake_data.failing_sample)
            self.no_genotype_reviewer = ar.SampleReviewer(fake_data.sample_no_genotype)

        with patch(ppath + 'clarity.get_species_from_sample', return_value='Bos taurus'):
            self.non_human_reviewer = ar.SampleReviewer(fake_data.non_human_sample)

    @patch(ppath + 'clarity.get_expected_yield_for_sample', return_value=95000000000)
    def test_failing_metrics(self, mocked_yield):
        for r in (self.passing_reviewer, self.no_genotype_reviewer, self.non_human_reviewer):
            assert r.get_failing_metrics() == []

        assert self.failing_reviewer.get_failing_metrics() == [
            'clean_yield_in_gb', 'clean_yield_q30', 'provided_gender'
        ]

    @patch(ppath + 'clarity.get_expected_yield_for_sample', return_value=120000000000)
    def test_cfg(self, mocked_yield):
        assert self.no_genotype_reviewer.cfg == {
            'clean_yield_in_gb': {'comparison': '>', 'value': 160},
            'pc_duplicate_reads': {'comparison': '<', 'value': 35},
            'pc_mapped_reads': {'comparison': '>', 'value': 90},
            'median_coverage': {'comparison': '>', 'value': 40},
            'clean_yield_q30': {'comparison': '>', 'value': 120},
            'provided_gender': {'value': {'key': 'called_gender', 'fallback': 'unknown'}, 'comparison': 'agreeswith'}
        }

        assert self.passing_reviewer.cfg == {
            'clean_yield_in_gb': {'comparison': '>', 'value': 160},
            'pc_duplicate_reads': {'comparison': '<', 'value': 35},
            'pc_mapped_reads': {'comparison': '>', 'value': 90},
            'median_coverage': {'comparison': '>', 'value': 40},
            'clean_yield_q30': {'comparison': '>', 'value': 120},
            'provided_gender': {'value': {'key': 'called_gender', 'fallback': 'unknown'}, 'comparison': 'agreeswith'},
            'genotype_validation.no_call_seq': {'comparison': '<', 'value': 10},
            'genotype_validation.mismatching_snps': {'comparison': '<', 'value': 5}
        }

    @patch(ppath + 'clarity.get_expected_yield_for_sample', return_value=95000000000)
    def test_summary(self, mocked_yield):
        assert self.failing_reviewer._summary == {
            'reviewed': 'fail',
            'review_comments': 'failed due to clean_yield_in_gb, clean_yield_q30, provided_gender'
        }
        assert self.no_genotype_reviewer._summary == {'reviewed': 'genotype missing'}
        assert self.non_human_reviewer._summary == {'reviewed': 'pass'}
        assert self.passing_reviewer._summary == {'reviewed': 'pass'}

    @patch(ppath + 'rest_communication.patch_entry')
    @patch(ppath + 'SampleReviewer.cfg', return_value=True)
    @patch(ppath + 'SampleReviewer._summary', new='a_payload')
    def test_push_review(self, mocked_cfg, mocked_patch):
        self.passing_reviewer.push_review()
        mocked_patch.assert_called_with('samples', 'a_payload', 'sample_id', 'LP1251551__B_12')


class TestRunReviewer(TestProjectManagement):
    def setUp(self):
        lanes = [fake_data.passing_lane] + fake_data.failing_lanes
        with patch(ppath + 'rest_communication.get_documents', return_value=lanes):
            self.reviewer = ar.RunReviewer(None)

    @patch(ppath + 'LaneReviewer._summary', new='a_payload')
    @patch(ppath + 'rest_communication.patch_entries')
    def test_review(self, mocked_patch):
        self.reviewer.push_review()
        for x in range(3):
            call = mocked_patch.call_args_list[x]
            assert call[0][0] == 'run_elements'
            assert call[1] == {'payload': 'a_payload', 'where': {'run_id': 'a_run', 'lane': x + 1}}
