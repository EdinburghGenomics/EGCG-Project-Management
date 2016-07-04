from tests import TestProjectManagement
from tests.test_automatic_review import fake_data
from bin import automatic_review


class TestAutomaticReview(TestProjectManagement):
    def test_run_review(self):
        for lane in fake_data.run_elements_by_lane_pass:
            run_review = automatic_review.metrics(lane, 'run')
            assert run_review == ['pass']
        for lane in fake_data.run_elements_by_lane_fail:
            run_review = automatic_review.metrics(lane, 'run')
            assert run_review[0] == 'fail'

    def test_sample_review(self):
        sample_review = automatic_review.metrics(fake_data.samples_fail, 'sample')
        review = sample_review[0]
        failed_fields = sample_review[1]
        assert review == 'fail'
        assert set(failed_fields) == {'clean_yield_q30', 'clean_yield_in_gb'}

        sample_review = automatic_review.metrics(fake_data.samples_pass, 'sample')
        review = sample_review[0]
        assert review == 'pass'